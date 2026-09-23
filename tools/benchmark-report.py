"""Build a reviewable benchmark report from durable ingestion summaries."""
import json
import os
from pathlib import Path

import boto3

root = Path(__file__).resolve().parents[1]
credentials = json.loads((root / ".local/credentials.json").read_text())
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT", "http://127.0.0.1:8333"),
    aws_access_key_id=credentials["s3_access"],
    aws_secret_access_key=credentials["s3_secret"],
    region_name="us-east-1",
)

runs = []
for prefix in ["runs/", "runs-dlt/"]:
    for item in s3.list_objects_v2(Bucket="metrics", Prefix=prefix).get("Contents", []):
        if item["Key"].endswith(".failed.json"):
            continue
        value = json.loads(s3.get_object(Bucket="metrics", Key=item["Key"])["Body"].read())
        if value.get("run_id") == "company-smoke" or value.get("run_id", "").startswith("verification-"):
            continue
        value.setdefault("engine", "spark")
        runs.append(value)

# Retries and repeated DAG runs are useful raw evidence. The comparison uses the
# latest successful result for each table/profile pair so it remains deterministic.
latest = {}
for run in sorted(runs, key=lambda value: value["started_at"]):
    latest[(run["engine"], run["profile"], run["table"])] = run

lines = [
    "# Benchmark local de ingestão\n",
    "Resultados produzidos pelas DAGs Airflow com Spark e dltHub, usando a mesma origem SQL Server e o mesmo storage Delta.\n",
    "| Motor | Perfil | Tabela | Linhas | Origem estimada (MiB) | Delta (MiB) | Partições | Estratégia | Leitura + escrita (s) | Linhas/s |",
    "|---|---|---:|---:|---:|---:|---:|---|---:|---:|",
]
for (engine, profile, table), run in sorted(latest.items()):
    lines.append(
        f"| {engine} | {profile} | {table} | {run['rows']:,} | "
        f"{run['source_estimated_bytes'] / 1048576:.2f} | {run['output_bytes'] / 1048576:.2f} | "
        f"{run['partitions']} | {run['plan']['strategy']} | "
        f"{run['durations_seconds']['read_write']:.2f} | {run['throughput_rows_per_second']:.1f} |"
    )

comparisons = []
for profile in ["small", "medium"]:
    for table in ["small", "medium", "wide"]:
        spark = latest.get(("spark", profile, table))
        dlt = latest.get(("dlt", profile, table))
        if spark and dlt:
            comparisons.append((profile, table, spark["throughput_rows_per_second"],
                                dlt["throughput_rows_per_second"]))

if comparisons:
    lines += [
        "\n## Comparação direta de throughput\n",
        "| Perfil | Tabela | Spark (linhas/s) | dltHub (linhas/s) | dltHub / Spark |",
        "|---|---|---:|---:|---:|",
    ]
    for profile, table, spark_rate, dlt_rate in comparisons:
        lines.append(
            f"| {profile} | {table} | {spark_rate:.1f} | {dlt_rate:.1f} | {dlt_rate / spark_rate:.2f}× |"
        )

lines += [
    "\nA origem representa páginas usadas estimadas pelo SQL Server. O tamanho Delta considera os arquivos de dados comprimidos, sem o transaction log.",
    "O tempo medido cobre extração e escrita Delta; inicialização do pod/cluster, planejamento e leitura de validação ficam separados nas métricas.",
    f"Ambiente local compartilhado: Minikube com 10 CPUs configuradas; host com {os.cpu_count()} CPUs lógicas. Os valores servem para comparação relativa, não como SLA de produção.\n",
]
out = root / "docs/benchmarks/results.md"
out.parent.mkdir(exist_ok=True)
out.write_text("\n".join(lines))
print(out)
