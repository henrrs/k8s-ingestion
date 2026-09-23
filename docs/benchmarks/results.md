# Benchmark local de ingestão

Resultados produzidos pelas DAGs Airflow com Spark e dltHub, usando a mesma origem SQL Server e o mesmo storage Delta.

| Motor | Perfil | Tabela | Linhas | Origem estimada (MiB) | Delta (MiB) | Partições | Estratégia | Leitura + escrita (s) | Linhas/s |
|---|---|---:|---:|---:|---:|---:|---|---:|---:|
| dlt | medium | medium | 200,000 | 68.27 | 15.82 | 1 | dlt_pyarrow_chunks | 9.70 | 20629.0 |
| dlt | medium | small | 10,000 | 0.81 | 0.25 | 1 | dlt_pyarrow_chunks | 2.18 | 4591.4 |
| dlt | medium | wide | 1,000,000 | 496.79 | 168.13 | 1 | dlt_pyarrow_chunks | 44.84 | 22302.5 |
| dlt | small | medium | 200,000 | 68.27 | 15.82 | 1 | dlt_pyarrow_chunks | 10.20 | 19612.9 |
| dlt | small | small | 10,000 | 0.81 | 0.25 | 1 | dlt_pyarrow_chunks | 2.29 | 4359.7 |
| dlt | small | wide | 1,000,000 | 496.79 | 168.13 | 1 | dlt_pyarrow_chunks | 44.42 | 22512.5 |
| spark | medium | medium | 200,000 | 68.27 | 15.39 | 8 | histogram_ranges | 20.70 | 9660.3 |
| spark | medium | small | 10,000 | 0.81 | 0.20 | 1 | single_scan | 13.56 | 737.6 |
| spark | medium | wide | 1,000,000 | 496.79 | 165.58 | 8 | histogram_ranges | 34.43 | 29043.2 |
| spark | small | medium | 200,000 | 68.27 | 15.22 | 4 | histogram_ranges | 28.29 | 7069.5 |
| spark | small | small | 10,000 | 0.81 | 0.20 | 1 | single_scan | 18.58 | 538.1 |
| spark | small | wide | 1,000,000 | 496.79 | 164.84 | 4 | histogram_ranges | 44.91 | 22267.7 |

## Comparação direta de throughput

| Perfil | Tabela | Spark (linhas/s) | dltHub (linhas/s) | dltHub / Spark |
|---|---|---:|---:|---:|
| small | small | 538.1 | 4359.7 | 8.10× |
| small | medium | 7069.5 | 19612.9 | 2.77× |
| small | wide | 22267.7 | 22512.5 | 1.01× |
| medium | small | 737.6 | 4591.4 | 6.22× |
| medium | medium | 9660.3 | 20629.0 | 2.14× |
| medium | wide | 29043.2 | 22302.5 | 0.77× |

A origem representa páginas usadas estimadas pelo SQL Server. O tamanho Delta considera os arquivos de dados comprimidos, sem o transaction log.
O tempo medido cobre extração e escrita Delta; inicialização do pod/cluster, planejamento e leitura de validação ficam separados nas métricas.
Ambiente local compartilhado: Minikube com 10 CPUs configuradas; host com 12 CPUs lógicas. Os valores servem para comparação relativa, não como SLA de produção.

## Aceite do dltHub distribuído

Estas execuções comparam o modelo `0.2.0`, que criava um pod para cada chunk,
com o modelo `0.3.0`, no qual cada pod permanece ativo e processa vários chunks.
Extração inclui criação dos workers, leitura SQL Server e escrita do staging. O
total do motor também inclui planejamento, validação e publicação Delta.

| Tabela | Chunks | Pods 0.2 | Pods 0.3 | Total 0.2 (s) | Total 0.3 (s) | Redução no total | Linhas/s 0.2 | Linhas/s 0.3 | Ganho de throughput |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| small | 1 | 1 | 1 | 10,71 | 12,21 | -14,0% | 1.061 | 1.018 | -4,1% |
| medium | 9 | 9 | 2 | 51,26 | 23,83 | 53,5% | 4.126 | 9.302 | 125,4% |
| wide | 63 | 63 | 4 | 172,21 | 70,16 | 59,3% | 6.144 | 16.484 | 168,3% |

O cenário `small` não se beneficia porque possui um único chunk e continua
usando um único pod; a diferença está dentro do custo variável de inicialização
do laboratório. No `medium`, a mudança reduziu nove inicializações para duas.
No `wide`, reduziu 63 inicializações para quatro, uma queda de 93,7% na
quantidade de pods workers.

### Planner e publicação adaptativos (`0.4.0`)

Em 22 de setembro de 2026, a `wide` foi repetida com os mesmos quatro workers
`small`, agora com `target_chunk_bytes=auto`, `fetch_size=auto` e publicação
Delta sem cópia quando o schema físico é compatível.

| Métrica | `0.3.0` fixo | `0.4.0` adaptativo | Variação |
|---|---:|---:|---:|
| Chunks | 63 | 8 | -87,3% |
| `target_chunk_bytes` | 8 MiB | 62,1 MiB | resolvido pelo planner |
| `fetch_size` | 50.000 | 32.201 | resolvido pelo planner |
| Extração | 60,67 s | 51,55 s | -15,0% |
| Publicação | 8,54 s | 1,14 s | -86,7% |
| Total do motor | 70,16 s | 53,12 s | -24,3% |
| Airflow ponta a ponta | 79,69 s | 63,75 s | -20,0% |
| Throughput | 16.484 linhas/s | 19.399 linhas/s | +17,7% |
| Dados Delta ativos | 182,91 MiB | 197,52 MiB | +8,0% |

O `publication_mode=zero_copy` registrou no log Delta os oito Parquets já
produzidos pelos workers. A execução anterior reescrevia os 198,60 MiB de
staging em um arquivo final de 182,91 MiB. A versão adaptativa evita essa segunda
escrita, em troca de manter oito arquivos e 14,61 MiB adicionais de dados ativos.

As séries cAdvisor coletadas durante cada janela mostram redução aproximada de
58,8% nos bytes recebidos, 74,8% nos bytes enviados e 31,5% nos bytes escritos
pelo pod do SeaweedFS. Esses contadores incluem interfaces, metadados e tráfego
interno do serviço; servem para comparação entre as duas execuções, não como
tamanho lógico da tabela.

### Diagnóstico interno dos workers

Uma execução instrumentada posterior manteve oito chunks e quatro workers. A
soma dos chunks foi 185,33 s de wall time e 177,41 s de CPU de processo, uma
ocupação de 95,7%. As fases internas do `pipeline.run` foram:

| Fase | Soma nos 8 chunks | Percentual do `pipeline.run` |
|---|---:|---:|
| dlt normalize + Parquet + upload | 136,76 s | 74,1% |
| fetch/decodificação SQL | 38,49 s | 20,9% |
| materialização `Row -> dict` | 8,67 s | 4,7% |
| conexão SQL | 0,62 s | 0,3% |
| execução inicial das queries | 0,01 s | <0,1% |

Os workers ficaram entre 75% e 88% de um core nas amostras cAdvisor, com picos
de 99,8%. SQL Server ficou em 0,09 core médio e SeaweedFS em 0,06 core médio.
Os chunks apresentaram coeficiente de variação de 3,7%, sem skew relevante. O
gargalo atual é CPU e transformação no processo dlt/Python; banco, storage,
memória e balanceamento ainda possuem folga no laboratório.

### Data plane colunar nativo (`0.5.0`)

Em 22 de setembro de 2026, o caminho quente foi trocado de
`pymssql -> SQLAlchemy Row -> dict -> normalizador dlt` para
`mssql-python/ODBC -> Arrow RecordBatch -> writer Parquet dlt`. SQLAlchemy e
`pymssql` permanecem somente no discovery e no planner. A comparação abaixo usa
a execução instrumentada `0.4.0` e a repetição final `0.5.0`, ambas com a mesma
tabela `wide`, oito chunks, quatro workers `small` e publicação sem cópia.

| Métrica | `0.4.0` por linhas | `0.5.0` colunar | Variação |
|---|---:|---:|---:|
| Extração | 56,60 s | 18,52 s | -67,3% |
| Total do motor | 58,15 s | 19,90 s | -65,8% |
| Airflow ponta a ponta | 66,92 s | 30,30 s | -54,7% |
| Throughput | 17.667 linhas/s | 54.005 linhas/s | +205,7% (3,06x) |
| CPU somada dos chunks | 177,41 s | 13,37 s | -92,5% |
| Wall time somado dos chunks | 185,33 s | 16,46 s | -91,1% |
| Fetch/decodificação SQL | 38,49 s | 5,30 s | -86,2% |
| Materialização `Row -> dict` | 8,67 s | 0,00 s | eliminada |
| dlt + Parquet + upload | 136,76 s | 10,62 s | -92,2% |
| Dados Delta ativos | 197,52 MiB | 176,13 MiB | -10,8% |

Uma execução imediatamente anterior da mesma versão terminou o motor em 17,16 s
e atingiu 63.669 linhas/s. A repetição final foi usada na tabela para evitar
selecionar apenas o melhor resultado; as duas concluíram no Airflow em cerca de
30,5 s.

O Delta final foi relido com 1.000.000 de linhas e as 48 colunas da origem. O
caminho legado produzia 50 colunas porque o normalizador acrescentava
`_dlt_load_id` e `_dlt_id`; o caminho Arrow preserva exatamente o schema de
origem, incluindo `decimal(28,6)`, timestamp UTC, Unicode e colunas esparsas.
Os oito Parquets somaram 184.688.594 bytes, com amplificação lógica de escrita
igual a 1,0 e nenhuma reescrita na publicação.

### Resultado detalhado do modelo persistente `0.3.0`

| Perfil | Tabela | Linhas | Chunks | Workers | Delta (MiB) | Extração (s) | Publicação (s) | Total (s) | Linhas/s | Retries |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| medium | small | 10.000 | 1 | 1 | 0,43 | 9,83 | 1,54 | 12,21 | 1.017,8 | 0 |
| small | medium | 200.000 | 9 | 2 | 19,85 | 21,50 | 1,85 | 23,83 | 9.301,6 | 0 |
| small | wide | 1.000.000 | 63 | 4 | 182,91 | 60,67 | 8,54 | 70,16 | 16.483,6 | 0 |

Comparado ao dltHub de um único pod, o modelo distribuído ainda tem overhead:
o `medium` levou 23,83 s contra 10,20 s, e o `wide` levou 70,16 s contra
44,42 s. A diferença do `wide` caiu de 3,88 vezes para 1,58 vez. Para volumes
maiores que a capacidade confortável de um pod, o modelo persistente oferece
paralelismo, checkpoints e retry parcial com um custo operacional bem menor que
o desenho anterior.

O Driver Job permaneceu em `small` (1 CPU/2 GiB). O perfil da tabela `small`
foi aplicado somente ao worker, que recebeu 4 CPUs/6 GiB. Todas as execuções
terminaram com contagem Delta igual à origem e zero retries.
