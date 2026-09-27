# E2E do AdaptiveIngestionOperator com Oracle

## Escopo

Validação executada em 27 de setembro de 2026 no Minikube `company-spark`, com
Airflow 3.3.2, provider CNCF Kubernetes 10.22.0, Oracle Free local, SeaweedFS e
`company-dlt-ingestion:0.6.0`.

O fluxo testado foi:

```text
Airflow DAG
  -> AdaptiveIngestionOperator
  -> KubernetesJobOperator deferrable
  -> Driver Job
  -> discovery e planner Oracle
  -> Indexed Job com quatro workers persistentes
  -> oito chunks Oracle Arrow
  -> Parquet no SeaweedFS
  -> commit Delta zero-copy
  -> readback do motor
  -> retomada da task pelo Triggerer
  -> DAG success
  -> leitura Delta independente
```

Não foi uma execução direta pelo script `run-oracle-e2e.py`. O run foi criado
pelo CLI do Airflow e agendado pelo `LocalExecutor`; o operador criou e
acompanhou o Driver Job usando a ServiceAccount `airflow`.

## Execução aceita

| Campo | Valor |
|---|---|
| DAG | `company_ingestion_oracle_adaptive` |
| Airflow run ID | `adaptive-e2e-rbac-20260927T222953Z` |
| Driver Job | `job-ai-ingest-oracl-da767cd1f9ba` |
| Worker Job | `workers-job-ai-ingest-oracl-da767cd1f9ba-fdbcf2e516` |
| Estado Airflow | `success` |
| Início Airflow | `2026-09-27 22:29:59.782266 UTC` |
| Fim Airflow | `2026-09-27 22:30:24.771551 UTC` |
| Duração Airflow | 24,99 s |

## Plano e resultado

| Métrica | Resultado |
|---|---:|
| Linhas estimadas na origem | 1.000.000 |
| Colunas | 48 |
| Bytes estimados na origem | 659.857.408 |
| Chunks | 8 |
| Workers persistentes | 4 |
| `target_chunk_bytes` adaptativo | 82.482.176 |
| `fetch_size` adaptativo | 25.420 linhas |
| Linhas gravadas | 1.000.000 |
| Linhas relidas pelo publisher | 1.000.000 |
| Arquivos Delta | 8 |
| Bytes de saída | 95.403.310 |
| Delta version | 0 |
| Modo de publicação | `zero_copy` |
| Write amplification de dados | 1,0 |
| Retries | 0 |
| Throughput | 80.574,73 linhas/s |

Tempos do motor:

| Fase | Segundos |
|---|---:|
| Planning | 0,239 |
| Extraction | 12,411 |
| Validation | 0,111 |
| Publication | 1,162 |
| Engine total | 13,947 |

Depois da conclusão, um pod separado abriu
`s3://lakehouse/oracle-airflow/bronze/wide/job-ai-ingest-oracl-da767cd1f9ba`
com `DeltaTable` e obteve:

```json
{"version": 0, "rows": 1000000, "files": 8}
```

Essa leitura é independente do `readback_rows` registrado pelo driver.

## Defeitos encontrados pelo E2E

O teste revelou quatro incompatibilidades que os testes de manifesto não
detectaram:

1. O operador reutilizava defaults de credenciais SQL Server em uma execução
   Oracle. O operador agora usa `secret_env=[]` e as DAGs declaram as referências
   específicas do conector.
2. `in_cluster=None` funcionava na criação síncrona, mas o trigger assíncrono
   tentava ler um kubeconfig. O default agora é `in_cluster=True`.
3. O nome longo do Driver Job fazia o nome do pod do Indexed Job ultrapassar 63
   caracteres. Driver e worker agora reservam espaço para o completion index.
4. O provider marca o pod durante cleanup/reattach. A Role do Airflow ganhou
   somente `patch` em `pods`; nenhuma permissão de Secrets foi adicionada.

Uma nova execução depois dos quatro ajustes concluiu sem erros RBAC.

## Limites desta validação

O teste usa Kubernetes Secrets e storage S3 compatível local. Ele não valida
Azure Key Vault, Workload Identity nem ADLS. Esses itens exigem um ambiente AKS
com identidade, RBAC Azure, rede e endpoints reais.
