# Comparação Spark × dltHub

O segundo caminho de execução testa quando uma ingestão não precisa pagar o
custo de inicialização e da distribuição do Spark.

```text
Airflow
├── CompanySparkOperator ── SparkApplication ── driver + executores ── Delta
└── EnterpriseK8sOperator ── Kubernetes Job ── dltHub + PyArrow ────── Delta
                                      │
SQL Server ───────────────────────────┴────────────── SeaweedFS S3
                                                     │
                                      Pushgateway → Prometheus → Grafana
```

As duas DAGs usam as mesmas tabelas SQL Server, perfis nominais, storage e
métricas. Cada execução recebe um caminho exclusivo e valida a contagem a partir
da tabela Delta persistida. O relatório usa a execução bem sucedida mais recente
para cada combinação de motor, perfil e tabela.

O significado de perfil não é idêntico. No Spark, `small` e `medium` controlam
driver, quantidade de executores, cores e slots do planner JDBC. No dltHub, o
perfil controla um único pod com 1 CPU/2 GiB ou 4 CPUs/6 GiB; a extração SQL usa
lotes PyArrow de 50.000 linhas e permanece em um processo. Isso é parte da
comparação arquitetural: dltHub reduz overhead para cargas simples, enquanto o
Spark consegue distribuir leitura e transformação.

A evolução aprovada para distribuir uma única tabela entre vários pods dltHub
mantém um único disparo no Airflow e cria um Driver Job segregado por execução.
O desenho, as regras de idempotência e os critérios de aceite estão em
[Ingestão dltHub distribuída com Driver Job](../architecture/distributed-driver-job.md).

`read_write` mede a ação de extração e commit Delta. Planejamento, startup do
pod/cluster e leitura de validação ficam separados. `source_estimated_bytes`
vem das páginas alocadas no SQL Server e não representa bytes trafegados.
`output_bytes` considera os Parquets ativos, sem `_delta_log`.

No laboratório, cada run é o único escritor de seu prefixo e delta-rs usa
`AWS_S3_ALLOW_UNSAFE_RENAME=true`. Em ADLS Gen2 essa configuração não é usada.
Para um S3 compartilhado em produção, configure o mecanismo de lock/commit
recomendado pelo backend antes de permitir escritores concorrentes.

Para executar:

1. Abra `http://localhost:8080`.
2. Dispare `company_ingestion_benchmark` com `table=all` para Spark.
3. Dispare `company_ingestion_dlt_benchmark` com os mesmos parâmetros.
4. Compare `engine`, `profile` e `table` em `http://localhost:3000/d/company-spark`.
5. Gere a tabela durável com `make benchmark-report`.
