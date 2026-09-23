# Company Spark local lab

Ambiente local para executar e comparar ingestões SQL Server → Delta Lake com
Airflow, Spark no Kubernetes e dltHub em Jobs genéricos no Minikube.

O código está separado por responsabilidade: `engines/spark/job` contém o
wheel Spark, `engines/dlt` contém a engine distribuída composável,
`libraries/ingestion-core` contém o planner puro compartilhado,
`orchestration/airflow` contém provider e DAGs, e `infrastructure/local`
contém somente o laboratório Minikube/SQL Server/SeaweedFS. Veja a
[organização completa](docs/architecture/repository-layout.md).

## Iniciar e observar

```bash
make local-up       # cria ou atualiza todo o laboratório
make ui             # restaura os túneis locais após reiniciar o Minikube
make status         # mostra pods e SparkApplications
```

As interfaces ficam disponíveis somente em `127.0.0.1`:

| Componente | Endereço | Uso |
|---|---|---|
| Airflow | http://localhost:8080 | Disparar as DAGs Spark e dltHub |
| Grafana | http://localhost:3000/d/company-spark | Comparar motor, perfil e tabela |
| Kubernetes Dashboard | http://localhost:8001 | Pods, deployments, uso de CPU/memória e logs do Minikube |
| Spark Live UI | http://localhost:4040 | Jobs, stages, SQL e executores durante a execução; o watcher inicia com `make ui` |
| Spark History Server | http://localhost:18080 | Jobs, stages, SQL plans e executores das aplicações concluídas |
| Prometheus | http://localhost:9090 | Consultas PromQL e inspeção das séries brutas |
| Pushgateway | http://localhost:9091 | Métricas publicadas por cada execução |
| SeaweedFS Filer | http://localhost:8888 | Interface web para navegar pelos arquivos armazenados |
| SeaweedFS S3 | `http://localhost:8333` | API S3 autenticada usada por Delta, eventos e resumos JSON |
| SQL Server | `localhost:1433` | Conexão via SSMS, Azure Data Studio, DBeaver ou `sqlcmd` |

Airflow e Grafana usam o usuário `admin`. O SQL Server de benchmark usa banco
`Benchmark`, schema `dbo` e usuário `ingestion`. As senhas locais são geradas
uma vez e ficam em [`.local/credentials.json`](.local/credentials.json), com
permissão `0600`. Para consultar uma senha sem imprimi-las todas:

```bash
jq -r '.airflow' .local/credentials.json
jq -r '.grafana' .local/credentials.json
jq -r '.sqlserver_ingestion' .local/credentials.json
```

No Airflow, abra a DAG `company_ingestion_benchmark`, escolha **Trigger DAG** e
informe `table` (`all`, `small`, `medium` ou `wide`) e `compute_profile`
(`small` ou `medium`). O usuário não informa coluna de particionamento nem número
de partições: o wheel consulta metadados, índices, tamanho e histogramas do SQL
Server para gerar o plano JDBC.

A DAG `company_ingestion_dlt_benchmark` usa o `EnterpriseK8sOperator` para
executar `company-dlt-ingestion:0.6.0` como um Kubernetes Job comum. Ela aceita
os mesmos valores de `table` e `compute_profile`, grava em
`s3://lakehouse/dlt/bronze/...` e permite comparar as métricas com o Spark.

A DAG `company_ingestion_dlt_distributed` usa uma única task Airflow para criar
um Driver Job. O driver planeja ranges automaticamente, cria um Indexed Job com
pods dltHub persistentes, distribui vários chunks para cada pod, valida os
checkpoints e publica uma única tabela Delta. Use `table`, `compute_profile` e
`max_workers`; coluna de divisão e quantidade de chunks são decididas pelo motor.

O comparativo produzido pelas execuções de aceite está em
[docs/benchmarks/results.md](docs/benchmarks/results.md). Ele pode ser regenerado
com `make benchmark-report` a partir dos resumos persistidos no SeaweedFS.

Referências do projeto:

- [Decisão da arquitetura composável](docs/architecture/composable-ingestion-engine.md)
- [Organização do repositório](docs/architecture/repository-layout.md)
- [Contrato e parâmetros do operador e motor](docs/reference/configuration.md)
- [Observabilidade e Spark UI ao vivo](docs/operations/observability.md)
- [Arquitetura e critérios da comparação Spark × dltHub](docs/benchmarks/spark-dlt-comparison.md)
- [Ingestão distribuída com Driver Job](docs/architecture/distributed-driver-job.md)

Para observar o cluster pelo terminal:

```bash
.tools/bin/kubectl --context company-spark -n spark-lab get pods -w
.tools/bin/kubectl --context company-spark -n spark-lab get sparkapplications -w
.tools/bin/kubectl --context company-spark top pods -n spark-lab
```

Os resumos duráveis ficam em `s3a://metrics/runs/` para Spark e
`s3://metrics/runs-dlt/` para dltHub; o estado do
Pushgateway também usa volume persistente. Os dados Delta ficam em
`s3a://lakehouse/bronze/<tabela>/<run_id>` para Spark e
`s3://lakehouse/dlt/bronze/<tabela>/<run_id>` para dltHub. Os eventos do Spark
ficam em `s3a://spark-events/`.

A porta `8333` não é uma tela de login. Ao abri-la diretamente no navegador, o
SeaweedFS responde `AccessDenied` porque a requisição S3 não contém assinatura.
Use a interface do filer na porta `8888` para navegação ou um cliente compatível
com S3 usando `s3_access` e `s3_secret` de `.local/credentials.json`.
