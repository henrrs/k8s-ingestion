"""Provision local benchmark dashboards and metrics collectors."""
import json
from pathlib import Path
import yaml

root = Path(__file__).resolve().parents[1]
ns = "spark-lab"
items = []
def obj(kind, name, **kw):
    api = "apps/v1" if kind == "Deployment" else "v1"
    items.append({"apiVersion": api, "kind": kind, "metadata": {"name": name, "namespace": ns}, **kw})
def deploy(name, image, port, args=None, env=None, config=None, mounts=None, memory="256Mi", extra_volumes=None, sa=None):
    volumes = [{"name": k, "configMap": {"name": v}} for k, v in (config or {}).items()] + (extra_volumes or [])
    spec = {"containers": [{"name": name, "image": image, "args": args or [], "env": env or [],
        "ports": [{"containerPort": port}], "volumeMounts": mounts or [],
        "resources": {"requests": {"cpu": "50m", "memory": memory}, "limits": {"memory": "768Mi"}}}], "volumes": volumes}
    if sa: spec["serviceAccountName"] = sa
    obj("Deployment", name, spec={"replicas": 1, "selector": {"matchLabels": {"app": name}},
        "template": {"metadata": {"labels": {"app": name}}, "spec": spec}})
    obj("Service", name, spec={"selector": {"app": name}, "ports": [{"port": port, "targetPort": port}]})

prom_config = {"global": {"scrape_interval": "10s"}, "scrape_configs": [
    {"job_name": "pushgateway", "honor_labels": True, "static_configs": [{"targets": ["pushgateway:9091"]}]},
    {"job_name": "cadvisor", "scheme": "https", "metrics_path": "/api/v1/nodes/company-spark/proxy/metrics/cadvisor",
     "authorization": {"credentials_file": "/var/run/secrets/kubernetes.io/serviceaccount/token"},
     "tls_config": {"ca_file": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"},
     "static_configs": [{"targets": ["kubernetes.default.svc:443"]}],
     "metric_relabel_configs": [{"source_labels": ["namespace"], "regex": "spark-lab", "action": "keep"}]},
]}
obj("ConfigMap", "prometheus-config", data={"prometheus.yml": yaml.safe_dump(prom_config)})
obj("ServiceAccount", "observability")
items.extend([
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": {"name": "company-observability"},
     "rules": [{"apiGroups": [""], "resources": ["nodes/proxy"], "verbs": ["get"]}]},
    {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": {"name": "company-observability"},
     "subjects": [{"kind": "ServiceAccount", "name": "observability", "namespace": ns}],
     "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "company-observability"}},
])
obj("PersistentVolumeClaim", "pushgateway-data", spec={"accessModes": ["ReadWriteOnce"],
    "resources": {"requests": {"storage": "256Mi"}}})
deploy("pushgateway", "prom/pushgateway:v1.11.3", 9091,
    args=["--persistence.file=/data/metrics"],
    mounts=[{"name": "data", "mountPath": "/data"}],
    extra_volumes=[{"name": "data", "persistentVolumeClaim": {"claimName": "pushgateway-data"}}],
    memory="32Mi")
deploy("prometheus", "prom/prometheus:v3.14.0", 9090,
    args=["--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.retention.time=7d", "--storage.tsdb.retention.size=300MB"],
    config={"config": "prometheus-config"}, mounts=[{"name": "config", "mountPath": "/etc/prometheus"}], sa="observability")

panels = []
def panel(title, expr, unit, x, y, width=8, table=False, description=""):
    panels.append({"id": len(panels)+1, "title": title, "description": description,
        "type": "table" if table else "timeseries", "datasource": {"type": "prometheus", "uid": "prometheus"},
        "gridPos": {"x": x, "y": y, "w": width, "h": 8},
        "targets": [{"expr": expr, "legendFormat": "{{engine}} / {{read_mode}} / {{table}} / {{profile}} / {{run_id}}", "refId": "A", "instant": table, "format": "table" if table else "time_series"}],
        "fieldConfig": {"defaults": {"unit": unit, "decimals": 2}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom"}, "tooltip": {"mode": "multi"}}})
selector = '{engine=~"$engine",table=~"$table",profile=~"$profile"}'
panel("Throughput da ingestão — registros/s", "company_ingestion_throughput_rows_per_second"+selector, "rps", 0,0,
      description="Linhas validadas / duração de extração e escrita Delta. Não inclui inicialização, planejamento ou leitura de validação.")
panel("Throughput de saída Delta — bytes/s", "company_ingestion_throughput_output_bytes_per_second"+selector, "Bps", 8,0,
      description="Bytes comprimidos dos arquivos de dados Delta / duração de extração+escrita. Não representa tráfego da origem.")
panel("Registros confirmados", "company_ingestion_rows"+selector, "short",16,0)
panel("Duração por fase", 'company_ingestion_duration_seconds{engine=~"$engine",table=~"$table",profile=~"$profile"}', "s",0,8,12)
panels[-1]["targets"][0]["legendFormat"] = "{{engine}} / {{table}} / {{profile}} / {{phase}}"
panel("Partições/paralelismo do motor", "company_ingestion_partitions"+selector, "short",12,8,12)
panel("Origem — bytes ESTIMADOS (páginas SQL Server)", "company_ingestion_source_estimated_bytes"+selector,"bytes",0,16,12)
panel("Destino — bytes efetivos de dados Delta", "company_ingestion_output_bytes"+selector,"bytes",12,16,12)
panel("Plano escolhido e justificativa", "company_ingestion_plan_info"+selector,"short",0,24,24,True)
panel("CPU por pod — cores utilizados", 'sum by(pod) (rate(container_cpu_usage_seconds_total{namespace="spark-lab",container!="",container!="POD"}[1m]))',"short",0,32,12)
panels[-1]["targets"][0]["legendFormat"]="{{pod}}"
panel("Memória por pod — working set", 'sum by(pod) (container_memory_working_set_bytes{namespace="spark-lab",container!="",container!="POD"})',"bytes",12,32,12)
panels[-1]["targets"][0]["legendFormat"]="{{pod}}"
panel("Chunks planejados", "max by(engine,table,profile,run_id) (company_ingestion_chunks_total"+selector+")", "short", 0,40,8,
      description="Quantidade de chunks duráveis produzidos pelo planner distribuído.")
panel("Duração por chunk", 'company_ingestion_chunk_duration_seconds{engine=~"$engine",table=~"$table",profile=~"$profile"}', "s", 8,40,8,
      description="Permite observar skew entre os ranges processados pelos pods dlt.")
panels[-1]["targets"][0]["legendFormat"] = "{{table}} / chunk {{chunk_id}}"
panel("Retries de chunks", "company_ingestion_retries_total"+selector, "short", 16,40,8)
panel("Progresso dos chunks", '100 * company_ingestion_chunks_succeeded{job="company_ingestion_progress",engine=~"$engine",table=~"$table",profile=~"$profile"} / company_ingestion_chunks_total{job="company_ingestion_progress",engine=~"$engine",table=~"$table",profile=~"$profile"}', "percent", 0,48,24,
      description="Percentual atualizado pelo Driver Job enquanto o Indexed Job executa.")
panel("Chunk alvo resolvido", "company_ingestion_target_chunk_bytes"+selector, "bytes", 0,56,8,
      description="Tamanho de origem por chunk resolvido pelo planner ou informado manualmente.")
panel("Fetch size resolvido", "company_ingestion_fetch_size"+selector, "short", 8,56,8,
      description="Quantidade de linhas por fetch resolvida pelo planner ou informada manualmente.")
panel("Amplificação lógica de escrita", "company_ingestion_data_write_amplification_ratio"+selector, "short", 16,56,8,
      description="Bytes Parquet de staging mais bytes reescritos na publicação, divididos pelos bytes Delta ativos.")
panel("Tempo interno dos chunks", 'sum by(phase) (company_ingestion_chunk_phase_duration_seconds{engine=~"$engine",table=~"$table",profile=~"$profile"})', "s", 0,64,24,
      description="Soma de CPU/tempo por operação dos chunks; use para distinguir JDBC, materialização Python e dlt/Parquet/upload.")
panels[-1]["targets"][0]["legendFormat"] = "{{phase}}"
spark_reader_selector = '{engine="spark",read_mode=~"$spark_read_mode",table=~"$table",profile=~"$profile"}'
panel("Spark JDBC × Arrow — pipeline produtivo", "company_ingestion_throughput_rows_per_second"+spark_reader_selector,
      "rps", 0,72,12,
      description="Comparação end-to-end até o commit Delta. Use esta métrica com isolate_io_phases=false para decidir performance.")
panel("Arrow — throughput observado na origem", "company_ingestion_reader_throughput_rows_per_second"+spark_reader_selector,
      "rps", 12,72,12,
      description="Linhas Arrow / span das tasks. Disponível no modo mssql_arrow; não inclui startup nem planejamento.")
panel("Spark reader — tempos internos", 'company_ingestion_duration_seconds{engine="spark",read_mode=~"$spark_read_mode",table=~"$table",profile=~"$profile",phase=~"reader_.*"}',
      "s", 0,80,24,
      description="Somas por executor podem se sobrepor. source_pipeline_span é o relógio de parede; fetch mede espera do SQL/ODBC e consumer_wait mede Arrow/Python/JVM e backpressure do Delta.")
panels[-1]["targets"][0]["legendFormat"] = "{{read_mode}} / {{phase}} / {{run_id}}"
panel("Arrow prefetch — espera por batch", 'company_ingestion_duration_seconds{engine="spark",read_mode="mssql_arrow",table=~"$table",profile=~"$profile",phase="reader_prefetch_queue_wait_seconds_sum"}',
      "s", 0,88,24,
      description="Tempo acumulado por task esperando um batch na fila limitada de prefetch. Compare com source_fetch_seconds_sum e consumer_wait.")
panels[-1]["targets"][0]["legendFormat"] = "{{table}} / {{run_id}}"
dashboard = {"uid": "company-spark", "title": "Company Ingestion — Spark × dltHub", "schemaVersion": 39, "version": 2,
    "refresh": "10s", "time": {"from": "now-6h", "to": "now"}, "panels": panels,
    "templating": {"list": [{"name": name, "type": "query", "datasource": {"type": "prometheus", "uid": "prometheus"},
        "query": f"label_values(company_ingestion_rows, {name})", "includeAll": True, "allValue": ".*", "multi": True,
        "current": {"text": "All", "value": "$__all"}, "refresh": 1} for name in ["engine", "table", "profile"]] + [{
            "name": "spark_read_mode", "type": "query",
            "datasource": {"type": "prometheus", "uid": "prometheus"},
            "query": 'label_values(company_ingestion_rows{engine="spark"}, read_mode)',
            "includeAll": True, "allValue": ".*", "multi": True,
            "current": {"text": "All", "value": "$__all"}, "refresh": 1,
        }]}}
out=root/"infrastructure/local/kubernetes/observability";out.mkdir(parents=True,exist_ok=True)
(out/"dashboard.json").write_text(json.dumps(dashboard,indent=2)+"\n")
obj("ConfigMap", "grafana-dashboard", data={"benchmark.json": json.dumps(dashboard)})
obj("ConfigMap", "grafana-datasources", data={"datasources.yaml": yaml.safe_dump({"apiVersion": 1,"datasources":[{"name":"Prometheus","type":"prometheus","uid":"prometheus","url":"http://prometheus:9090","access":"proxy","isDefault":True}]})})
obj("ConfigMap", "grafana-providers", data={"dashboards.yaml": yaml.safe_dump({"apiVersion":1,"providers":[{"name":"Company Spark","type":"file","options":{"path":"/etc/grafana/dashboards"}}]})})
deploy("grafana", "grafana/grafana:13.2.2",3000,
    env=[{"name":"GF_SECURITY_ADMIN_USER","value":"admin"},{"name":"GF_SECURITY_ADMIN_PASSWORD","valueFrom":{"secretKeyRef":{"name":"grafana-credentials","key":"password"}}},
         {"name":"GF_USERS_DEFAULT_THEME","value":"light"}],
    config={"dashboards":"grafana-dashboard","datasources":"grafana-datasources","providers":"grafana-providers"},
    mounts=[{"name":"dashboards","mountPath":"/etc/grafana/dashboards"},{"name":"datasources","mountPath":"/etc/grafana/provisioning/datasources"},
            {"name":"providers","mountPath":"/etc/grafana/provisioning/dashboards"}],memory="128Mi")

history_opts = " ".join(["-Dspark.history.fs.logDirectory=s3a://spark-events/", "-Dspark.hadoop.fs.s3a.endpoint=http://seaweedfs:8333",
    "-Dspark.hadoop.fs.s3a.path.style.access=true", "-Dspark.hadoop.fs.s3a.connection.ssl.enabled=false", "-Dspark.hadoop.fs.s3a.endpoint.region=us-east-1",
    "-Dspark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider"])
deploy("spark-history", "company-spark-runtime:0.4.0",18080,
    env=[{"name":"SPARK_HISTORY_OPTS","value":history_opts},{"name":"SPARK_DAEMON_MEMORY","value":"512m"},
         {"name":"AWS_ACCESS_KEY_ID","valueFrom":{"secretKeyRef":{"name":"seaweedfs-credentials","key":"access_key"}}},
         {"name":"AWS_SECRET_ACCESS_KEY","valueFrom":{"secretKeyRef":{"name":"seaweedfs-credentials","key":"secret_key"}}}],memory="256Mi")
for item in items:
    if item["kind"]=="Deployment" and item["metadata"]["name"]=="spark-history":
        item["spec"]["template"]["spec"]["containers"][0]["command"]=["/opt/spark/bin/spark-class","org.apache.spark.deploy.history.HistoryServer"]
        item["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"]="IfNotPresent"
(out/"manifests.yaml").write_text(yaml.safe_dump_all(items,sort_keys=False))
print(out)
