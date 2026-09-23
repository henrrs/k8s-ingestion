# Observabilidade das execuções

## O que existe hoje

| Momento | Interface | Informação disponível |
|---|---|---|
| Antes e durante | Airflow | Estado da DAG/task, aplicação criada, retries e timeout. |
| Durante | Spark Web UI do driver | Jobs, stages, tasks, SQL, executores, memória, shuffle e event timeline em tempo real. |
| Durante | Kubernetes Dashboard | Pods, eventos, logs, CPU e memória do driver/executores. |
| Durante | Grafana | CPU/memória e, no dlt distribuído, chunks planejados, ativos, concluídos, falhos e progresso percentual. |
| Durante e depois | Logs do driver | Plano adaptativo, erros e resultado final. |
| Depois | Spark History Server | Reconstrução da UI Spark a partir dos event logs no SeaweedFS. |
| Depois | Grafana/Prometheus | Linhas, bytes, partições, estratégia, fases, throughput, parâmetros adaptativos e amplificação de escrita. |

Jobs do `EnterpriseK8sOperator` não possuem Spark Web UI, pois não executam um
Spark driver. Durante a execução dltHub, acompanhe a task e seus logs no Airflow,
o Job/pod no Kubernetes Dashboard e CPU/memória no Grafana. O caminho distribuído
também publica o progresso dos chunks no Pushgateway a cada mudança observada
pelo driver e publica duração/sucesso por chunk quando cada worker termina. As
métricas finais ganham o label `engine`, que permite comparar `spark`, `dlt` e
`dlt-distributed` no mesmo painel.

Cada chunk distribuído também separa conexão e execução SQL, fetch, conversão
para objetos Python, setup do pipeline, normalização/Parquet/upload e listagem de
objetos. O painel `Tempo interno dos chunks` mostra a soma dessas fases.

O History Server pode listar aplicações incompletas conforme relê os event logs,
mas essa atualização é periódica. Para acompanhamento realmente vivo, a fonte
correta é a Web UI do próprio driver Spark na porta `4040`.

## Acessar a UI Spark viva no laboratório

O comando geral de interfaces também inicia um watcher para drivers Spark:

```bash
make ui
```

Depois, dispare uma task no Airflow e abra http://localhost:4040. O watcher
espera a UI ficar pronta, conecta ao driver atual e acompanha a próxima task da
DAG quando o pod muda.

Para criar o túnel manualmente, sem o watcher:

```bash
make spark-ui
```

O comando localiza o driver mais recente em execução. Para escolher uma
aplicação específica:

```bash
make spark-ui APPLICATION=company-ingest-wide-4e807439741764f3
```

Quando não existe driver ativo, a porta 4040 não tem conteúdo. Isso é uma
característica da UI viva: seu processo pertence à aplicação. Ao finalizar uma
task, atualize a página quando a próxima começar; o watcher troca o túnel
automaticamente. Depois que a DAG terminar, use http://localhost:18080 no
History Server.

Para logs em streaming:

```bash
.tools/bin/kubectl --context company-spark -n spark-lab logs -f <driver-pod>
```

## Experiência recomendada para AKS

Não é necessário desenvolver outra implementação da Spark UI. A arquitetura
recomendada é publicar a UI nativa de cada driver:

1. A `SparkApplication` cria um Service por execução para a porta 4040.
2. Um Ingress/Gateway privado roteia `/spark/<application-name>` para o Service.
3. O gateway autentica usuários com Entra ID/OIDC e aplica autorização.
4. O `CompanySparkOperator` registra um Airflow Operator Extra Link chamado
   `Spark UI` apontando para essa rota.
5. Durante a execução o link abre o driver; após o término abre a aplicação no
   History Server.

O CRD oficial instalado já oferece `driverServiceIngressList`, que permite ao
operador declarar Service e Ingress junto da aplicação. Falta implementar no
provider a política de URL, autenticação e o Extra Link do Airflow. Essa camada é
um gateway fino sobre a UI Spark existente, e não uma nova interface de jobs.

Nunca exponha a porta 4040 diretamente à internet. A UI revela configuração,
planos, nomes de tabelas e informações operacionais; no AKS ela deve permanecer
privada e protegida pelo gateway de identidade.
