# Contrato de execução e parâmetros

Este documento descreve a interface pública entre a DAG, o
`CompanySparkOperator`, o `EnterpriseK8sOperator`, os runtimes e os motores de
ingestão. A DAG informa intenção e limites; cada motor decide como executar.

## CompanySparkOperator

| Parâmetro | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `task_id` | sim | — | Identificador da task Airflow. |
| `parameters` | sim | — | Contrato do motor descrito abaixo. É um campo templated. |
| `wheel_url` | sim | — | URL HTTP(S) do wheel executado pelo runtime. |
| `wheel_sha256` | recomendado | `None` | SHA-256 esperado. Quando informado, divergência impede a execução. |
| `compute_profile` | não | `small` | Perfil `small` ou `medium`. É um campo templated. |
| `runtime` | não | `company-spark-runtime:0.1.0` | Imagem genérica de Spark usada por driver e executores. |
| `spark_version` | não | `4.2.0` | Versão declarada na `SparkApplication`. |
| `namespace` | não | `spark-lab` | Namespace onde a aplicação é criada. |
| `entrypoint` | não | `company_ingestion.entrypoint:main` | Função Python carregada do wheel. |
| `spark_conf` | não | `{}` | Overrides avançados de configurações Spark. |
| `timeout_seconds` | não | `3600` | Prazo total acompanhado pelo trigger. |
| `poll_interval` | não | `10` | Intervalo de consulta da `SparkApplication`. |
| `in_cluster` | não | `True` | Usa credenciais Kubernetes do pod do Airflow. |
| `kube_context` | não | `None` | Contexto usado quando `in_cluster=False`. |
| `enabled` | não | `True` | Quando falso, marca a task como skipped sem criar aplicação. |

Parâmetros Airflow usuais, como `retries`, `execution_timeout` e
`trigger_rule`, também podem ser fornecidos. O nome da aplicação deriva de
`dag_id`, `task_id`, `run_id` e número da tentativa. A mesma tentativa é
reattachable; um retry cria outro nome.

`spark_conf` tem precedência sobre a configuração gerada. Ele deve ser usado
somente para opções técnicas. Segredos não devem aparecer em `parameters` ou
`spark_conf`; são referenciados por Kubernetes Secrets.

## Perfis de computação

| Perfil | Driver | Executores | Total de slots usado pelo planner |
|---|---|---|---:|
| `small` | 1 core, 1 GiB | 2 × 1 core, 1 GiB | 2 |
| `medium` | 1 core, 1 GiB | 2 × 2 cores, 2 GiB | 4 |

Cada container recebe mais 512 MiB de memory overhead. Os perfis ficam em
`orchestration/airflow/provider/src/company_airflow/application.py` e podem evoluir
sem alterar as DAGs.

## EnterpriseK8sOperator

Este operador executa uma imagem versionada como um `batch/v1 Job` comum. Ele
não depende do Spark Operator e pode hospedar dltHub, dbt, um binário Go ou
outro workload batch que respeite o contrato de resultado.

| Parâmetro | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `task_id` | sim | — | Identificador da task Airflow. |
| `image` | sim | — | Imagem já disponível no registry ou no node local. |
| `configuration` | sim | — | Objeto serializado em `COMPANY_JOB_CONFIG`. |
| `compute_profile` | não | `small` | `small` = 1 CPU/2 GiB; `medium` = 4 CPUs/6 GiB. |
| `command` | não | entrypoint da imagem | Sobrescreve o entrypoint do contêiner. |
| `arguments` | não | argumentos da imagem | Argumentos do processo. |
| `env` | não | `{}` | Variáveis não secretas. É um campo templated. |
| `secret_env` | não | `[]` | Lista de `{name, secret, key}` para Kubernetes Secrets. |
| `namespace` | não | `spark-lab` | Namespace do Job. |
| `timeout_seconds` | não | `3600` | Deadline do Job e prazo acompanhado pelo trigger. |
| `poll_interval` | não | `5` | Intervalo de consulta assíncrona. |
| `result_marker` | não | `COMPANY_JOB_RESULT=` | Prefixo da linha JSON retornada em XCom. |
| `in_cluster` | não | `True` | Usa a ServiceAccount do Airflow. |
| `kube_context` | não | `None` | Contexto para execução fora do cluster. |
| `enabled` | não | `True` | Quando falso, marca a task como skipped. |

O nome do Job deriva de DAG, task, run e tentativa. Uma retomada da mesma
tentativa se reconecta ao Job existente; um retry recebe outro nome. O operador
usa `backoffLimit=0`, define CPU e memória como request/limit e remove o Job um
dia após a conclusão. Logs e o JSON final continuam acessíveis no Airflow.

Exemplo mínimo:

```python
EnterpriseK8sOperator(
    task_id="ingest_orders_dlt",
    image="company-dlt-ingestion:0.6.0",
    compute_profile="small",
    configuration={
        "source": {"type": "sqlserver", "host": "sqlserver", "port": 1433,
                   "database": "Sales", "schema": "dbo", "table": "orders"},
        "destination": {"format": "delta", "uri": "s3://lakehouse/dlt/bronze/orders"},
        "storage": {"endpoint_url": "http://seaweedfs:8333"},
        "extract": {"backend": "pyarrow", "chunk_size": 50000},
        "metrics": {"pushgateway": "http://pushgateway:9091"},
    },
    secret_env=[
        {"name": "SQLSERVER_USER", "secret": "sqlserver-credentials", "key": "username"},
        {"name": "SQLSERVER_PASSWORD", "secret": "sqlserver-credentials", "key": "password"},
    ],
)
```

## EnterpriseIngestionOperator

Este operador cria um único Driver Job por execução. O driver faz discovery da
origem, persiste um plano imutável, cria o Indexed Job dos workers dltHub,
valida os manifests e publica o Delta. O plano não retorna ao Airflow entre as
fases.

| Parâmetro | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `task_id` | sim | — | Identificador da task Airflow. |
| `source` | sim | — | Origem SQL Server descrita abaixo. |
| `destination` | sim | — | Destino Delta. |
| `execution` | não | `{}` | Limites do planner, workers, chunks e arquivos. |
| `storage` | não | SeaweedFS local | Configuração do object storage. |
| `metrics` | não | `{}` | Summary URI e Pushgateway. |
| `validation` | não | `{}` | Política de validação do snapshot. |
| `image` | não | `company-dlt-ingestion:0.6.0` | Imagem comum do driver e workers. |
| `compute_profile` | não | `small` | Recursos de cada worker; o driver permanece em `small`. |
| `namespace` | não | `spark-lab` | Namespace do driver e Indexed Job. |
| `driver_service_account` | não | `ingestion-driver` | ServiceAccount que pode criar e acompanhar Jobs filhos. |
| `worker_service_account` | não | `ingestion-worker` | ServiceAccount sem permissão para criar workloads. |
| `secret_env` | não | Secrets locais SQL/S3 | Referências propagadas ao driver e aos workers. |
| `timeout_seconds` | não | `7200` | Deadline do driver e do Worker Job. |

### `execution`

| Campo | Padrão | Descrição |
|---|---:|---|
| `max_workers` | `4` | Limite de pods dltHub persistentes desta execução. |
| `max_source_connections` | `4` | Limite de consultas simultâneas ao banco. |
| `max_chunks` | `128` | Limite de chunks duráveis do snapshot. |
| `extract_backend` | automático por origem | SQL Server usa `mssql_arrow`; Oracle usa `oracle_arrow`. `sqlalchemy_rows` preserva o caminho legado SQL Server `Row -> dict` para diagnóstico. |
| `encoder` | `dlt_parquet` | Estratégia que transforma os lotes da origem em artefatos Parquet. Esta versão registra somente `dlt_parquet`. |
| `target_chunk_bytes` | `auto` | `auto` usa linhas, bytes estimados e concorrência; um inteiro força o tamanho desejado. |
| `fetch_size` | `auto` | `auto` calcula as linhas por lote a partir da largura média estimada. |
| `target_file_bytes` | `268435456` | Tamanho alvo usado quando a publicação precisa normalizar e reescrever arquivos. |
| `publication_mode` | `auto` | Registra os Parquets diretamente no Delta quando são compatíveis; `rewrite` força normalização. |
| `chunk_retries` | `2` | Tentativas adicionais permitidas por índice. |
| `control_uri` | `s3://ingestion-control/runs` | Raiz de planos, manifests e resultados. |
| `staging_uri` | `<destination.uri>/<run_id>/_staging` | Raiz dos Parquets imutáveis, dentro da tabela para publicação sem cópia. |

`max_workers` e `max_source_connections` são limites. O planner pode escolher
um paralelismo menor. Em modo automático, tabelas de até 128 MiB usam um chunk.
Acima disso, o planner busca chunks de 256 MiB, garante até dois chunks por
conexão para balanceamento, evita chunks menores que 32 MiB ou 100 mil linhas e
limita chunks a 4 GiB ou 5 milhões de linhas para preservar retries. Todos esses
limites respeitam `max_chunks`.

O `fetch_size` automático reserva aproximadamente 16 MiB de dados de origem por
lote: `16 MiB / bytes médios por linha`, limitado entre 1.000 e 100.000 linhas e
pela cardinalidade estimada do chunk. Os valores resolvidos são persistidos em
`plan.json`; retries reutilizam exatamente o mesmo plano.

`validation.require_source_row_match` assume `true`. Quando ativo, o publisher
compara a contagem Delta com a contagem observada pelo planner antes de concluir.
Por isso, o MVP local pressupõe que a tabela permaneça estável durante a carga
full. Tabelas mutáveis precisam de CDC, snapshot isolation ou uma fronteira de
watermark antes da promoção produtiva.

### Exemplo distribuído

```python
EnterpriseIngestionOperator(
    task_id="ingest_orders",
    image="company-dlt-ingestion:0.6.0",
    compute_profile="small",
    source={
        "type": "sqlserver", "host": "sqlserver", "port": 1433,
        "database": "Sales", "schema": "dbo", "table": "orders",
    },
    destination={
        "format": "delta",
        "uri": "s3://lakehouse/dlt-distributed/bronze/orders",
    },
    execution={
        "max_workers": 4,
        "max_source_connections": 4,
        "max_chunks": 64,
        "extract_backend": "mssql_arrow",
        "target_chunk_bytes": "auto",
        "fetch_size": "auto",
    },
    storage={"type": "s3", "endpoint_url": "http://seaweedfs:8333"},
    metrics={
        "output_uri": "s3://metrics/runs-dlt-distributed",
        "pushgateway": "http://pushgateway:9091",
    },
)
```

O contrato detalhado de estado, retry, RBAC e publicação está em
[Ingestão dltHub distribuída com Driver Job](../architecture/distributed-driver-job.md).

## AdaptiveIngestionOperator

`AdaptiveIngestionOperator` é a evolução do operador distribuído sobre o
`KubernetesJobOperator` oficial. Ele preserva o contrato de um Driver Job por
execução e delega ao provider CNCF Kubernetes a criação, observação, logs e
deferral do Job.

Implementação e integração:

| Peça | Arquivo |
|---|---|
| Operador Airflow | `orchestration/airflow/provider/src/company_airflow/operators/adaptive_ingestion.py` |
| Configuração driver/workers | `orchestration/airflow/provider/src/company_airflow/ingestion.py` |
| Manifesto do Indexed Job | `engines/dlt/src/company_dlt_ingestion/infrastructure/kubernetes.py` |
| DAG Oracle executável | `orchestration/airflow/dags/company_ingestion_oracle_adaptive.py` |
| Testes do operador | `orchestration/airflow/provider/tests/test_adaptive_ingestion_operator.py` |

| Parâmetro | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `source` | sim | — | Origem declarativa. |
| `destination` | sim | — | Destino declarativo. |
| `execution` | não | `{}` | Limites do planner e URIs de controle. |
| `storage` | não | `{}` | Configuração do store atual. |
| `metrics` | não | `{}` | Destino das métricas. |
| `validation` | não | `{}` | Política de validação. |
| `image` | não | `company-dlt-ingestion:0.6.0` | Runtime comum de driver e workers. |
| `compute_profile` | não | `small` | Recursos dos workers. |
| `namespace` | não | `spark-lab` | Namespace dos Jobs. |
| `driver_service_account` | não | `ingestion-driver` | ServiceAccount do driver. |
| `worker_service_account` | não | `ingestion-worker` | ServiceAccount dos workers. |
| `credential_mode` | não | `kubernetes_secret` | `kubernetes_secret` no laboratório ou `workload_identity` no contrato produtivo. |
| `secret_env` | não | `[]` | Referências `{name, secret, key}`. A DAG declara nomes específicos do conector; lista vazia em Workload Identity. |
| `kubernetes_conn_id` | não | `kubernetes_default` | Connection usada pelo KubernetesHook. |
| `deferrable` | não | `True` | Libera o worker Airflow durante a execução. |
| `timeout_seconds` | não | `7200` | Deadline do Driver e Worker Jobs. |

```python
from company_airflow.operators import AdaptiveIngestionOperator

AdaptiveIngestionOperator(
    task_id="ingest_orders",
    source={
        "type": "oracle",
        "host": "oracle",
        "service_name": "FREEPDB1",
        "schema": "BENCHMARK",
        "table": "WIDE",
    },
    destination={
        "format": "delta",
        "uri": "s3://lakehouse/adaptive/oracle-wide",
    },
    execution={
        "max_workers": 4,
        "max_source_connections": 4,
        "target_chunk_bytes": "auto",
        "fetch_size": "auto",
    },
    compute_profile="medium",
    credential_mode="kubernetes_secret",
    secret_env=[
        {"name": "ORACLE_USER", "secret": "oracle-credentials", "key": "username"},
        {"name": "ORACLE_PASSWORD", "secret": "oracle-credentials", "key": "password"},
    ],
)
```

O nome do Job exclui `try_number`. Um retry da mesma execução Airflow reanexa
ao recurso existente quando o hash da configuração coincide. O retorno contém
`job`, `namespace`, `profile` e `result_uri`; XCom sidecar não é usado, evitando
a permissão `pods/exec`.

No modo `workload_identity`, o operador aplica o label exigido pela Azure ao
driver e propaga a política ao template dos workers. Esse modo depende da futura
implementação do `SecretResolver` do Azure Key Vault e do store ADLS; a simples
geração dos manifests não faz o runtime atual resolver secrets do Key Vault.

O E2E local completo e seus números estão em
[E2E do AdaptiveIngestionOperator com Oracle](../benchmarks/adaptive-airflow-oracle-e2e.md).

## Contrato do mini motor dltHub

O contêiner em `engines/dlt` aceita SQL Server e Oracle como origens e Delta em storage
S3 compatível como destino. O caminho efetivo é `<destination.uri>/<run_id>`.

| Seção/campo | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `source.type` | sim | — | `sqlserver` ou `oracle`. |
| `source.host` | sim | — | DNS/IP da origem. |
| `source.port` | não | `1433` | Porta TDS. |
| `source.database` | sim | — | Banco. |
| `source.schema` | sim | — | Schema. |
| `source.table` | sim | — | Tabela integral. |
| `destination.format` | sim | — | Deve ser `delta`. |
| `destination.uri` | sim | — | Prefixo `s3://` da tabela. |
| `storage.endpoint_url` | não | `http://seaweedfs:8333` | Endpoint S3 compatível. |
| `extract.backend` | não | `pyarrow` | Backend do benchmark. |
| `extract.chunk_size` | não | `50000` | Linhas por lote extraído. |
| `metrics.pushgateway` | não | nenhum | Endpoint de métricas. |

As credenciais chegam somente por variáveis referenciadas em Kubernetes Secrets:
`SQLSERVER_USER`/`SQLSERVER_PASSWORD` ou `ORACLE_USER`/`ORACLE_PASSWORD`, além de
`AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY`. O motor consulta contagem, páginas
usadas e colunas, extrai em lotes PyArrow, faz o commit Delta, relê a tabela e
publica o mesmo conjunto de métricas do Spark com `engine="dlt"`.

## Contrato `parameters` do motor

O operador injeta `run_id`, `profile` e `planner.task_slots`. A DAG fornece as
seções seguintes.

### `source`

| Campo | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `type` | sim | — | Tecnologia da origem: `sqlserver` ou `oracle`. |
| `host` | sim | — | Host DNS ou IP do SQL Server. |
| `port` | não | `1433` | Porta TCP/TDS. |
| `database` | sim | — | Banco de dados. |
| `schema` | sim | — | Schema da tabela. |
| `table` | sim | — | Tabela lida integralmente. |
| `user_env` | não | `SQLSERVER_USER` | Variável de ambiente que contém o usuário. |
| `password_env` | não | `SQLSERVER_PASSWORD` | Variável que contém a senha. |
| `encrypt` | não | `true` | Ativa criptografia JDBC. |
| `trust_server_certificate` | não | `false` | Aceita certificado não validado; útil somente no laboratório. |
| `query_timeout_seconds` | não | `600` | Timeout das consultas de metadados. |

O usuário precisa de `SELECT`, `VIEW DEFINITION` e permissão para consultar
as DMVs de tamanho utilizadas pelo planner.

### Campos de origem Oracle

| Campo | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `type` | sim | — | Deve ser `oracle`. |
| `host` | sim | — | Host DNS ou IP do listener. |
| `port` | não | `1521` | Porta TCP do listener. |
| `service_name` | sim* | `source.database` | Service name, como `FREEPDB1`. Um dos dois campos deve existir. |
| `database` | sim* | `source.service_name` | Nome lógico persistido nas métricas; também pode fornecer o service name. |
| `schema` | sim | — | Owner da tabela. Identificadores comuns são normalizados para maiúsculas. |
| `table` | sim | — | Tabela integral. |
| `preserve_identifier_case` | não | `false` | Preserva a caixa para objetos criados com identificadores entre aspas. |
| `user_env` | não | `ORACLE_USER` | Variável que contém o usuário. |
| `password_env` | não | `ORACLE_PASSWORD` | Variável que contém a senha. |
| `query_timeout_seconds` | não | `600` | Timeout de cada chamada ao banco. |

O usuário Oracle precisa consultar a tabela e as views `ALL_TABLES`,
`ALL_TAB_COLUMNS`, `ALL_INDEXES`, `ALL_IND_COLUMNS`, `ALL_CONSTRAINTS` e
`ALL_OBJECTS`, além de executar `DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER`. Cada
worker lê a origem `AS OF SCN` usando o SCN gravado no plano.

### `destination`

| Campo | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `uri` | sim | — | Prefixo da tabela, por exemplo `s3a://lakehouse/bronze/orders`. |
| `format` | não | `delta` | Esta versão aceita somente `delta`. |

O caminho efetivo é `<uri>/<run_id>`. O motor usa `errorifexists`, impedindo que
uma tentativa sobrescreva silenciosamente outra execução.

### `planner`

| Campo | Obrigatório | Padrão | Descrição |
|---|---:|---:|---|
| `max_connections` | não | `8` | Limite de conexões JDBC concorrentes por execução. |
| `target_partition_bytes` | não | `33554432` | Tamanho estimado desejado por partição (32 MiB). |
| `fetch_size` | não | `10000` | Linhas solicitadas por lote JDBC. |
| `task_slots` | injetado | perfil | Paralelismo físico disponível; a DAG não deve defini-lo. |

O número solicitado é limitado por conexões, capacidade do perfil e tamanho:

```text
min(max_connections, task_slots × 2, ceil(source_bytes / target_partition_bytes))
```

Para mais de uma partição, o motor procura a primeira chave numérica ou temporal
de um índice habilitado e sem filtro, priorizando índice clustered, unique e
primary key. Histogramas do SQL Server geram faixas balanceadas. Se não houver
histograma utilizável, são usadas faixas por `MIN/MAX`. Sem chave segura, o motor
faz uma única leitura para não repetir full scans. `partitionColumn`,
`lowerBound`, `upperBound` e `numPartitions` não fazem parte do contrato público.

### `metrics`

| Campo | Obrigatório | Padrão | Descrição |
|---|---:|---|---|
| `output_uri` | não | `s3a://metrics/runs` | Local dos resumos JSON duráveis. |
| `pushgateway` | não | `None` | Endpoint Prometheus Pushgateway. |

O resultado contém contagens, bytes estimados da origem, bytes Delta reais,
número de arquivos e partições, estratégia, checksum de leitura, versão Delta,
durações por fase e throughput. O mesmo objeto é retornado via XCom.

## Exemplo de DAG

```python
CompanySparkOperator(
    task_id="ingest_orders",
    compute_profile="medium",
    wheel_url="http://artifacts:8080/company_ingestion-0.6.0-py3-none-any.whl",
    wheel_sha256=os.environ["COMPANY_WHEEL_SHA256"],
    parameters={
        "source": {
            "type": "sqlserver",
            "host": "sqlserver",
            "database": "Sales",
            "schema": "dbo",
            "table": "orders",
        },
        "destination": {
            "format": "delta",
            "uri": "s3a://lakehouse/bronze/orders",
        },
        "planner": {
            "max_connections": 8,
            "target_partition_bytes": 33554432,
        },
        "metrics": {
            "output_uri": "s3a://metrics/runs",
            "pushgateway": "http://pushgateway:9091",
        },
    },
)
```

## Responsabilidade de cada peça

| Peça | Responsabilidade |
|---|---|
| DAG | Escolher origem, destino, perfil, wheel e política Airflow. |
| `CompanySparkOperator` | Criar/acompanhar a SparkApplication e devolver o resultado. |
| `CompanySparkRuntime` | Fornecer Spark, Java, Delta, Hadoop e drivers; baixar e validar o wheel. |
| `company_ingestion_core` | Fornecer modelos e algoritmos puros do planner adaptativo às duas engines. |
| `company_ingestion` | Executar a ingestão Spark JDBC → Delta como wheel carregado pelo runtime. |
| Spark Operator | Reconciliar driver, executores e estado Kubernetes. |
| `EnterpriseK8sOperator` | Criar/acompanhar um Job genérico, coletar logs e devolver o resultado. |
| `company_dlt_ingestion` | Compor adapters e coordenar driver/workers dltHub, checkpoints e publicação Delta. |
| Kubernetes Job controller | Agendar o pod, aplicar deadline e registrar o estado terminal. |
