# Autenticação do motor no Azure Key Vault

## Objetivo e fronteiras

O motor precisa obter credenciais de origem em tempo de execução sem colocar o
valor do segredo na DAG, no plano distribuído, no manifesto do Job ou no object
storage. Autenticação no Microsoft Entra ID e autorização no Key Vault são
controles diferentes:

- a identidade prova ao Entra quem é o workload;
- Azure RBAC autoriza essa identidade a executar `secrets/get` no vault;
- rede e DNS permitem alcançar Entra e Key Vault;
- Kubernetes RBAC controla criação e observação de Jobs e não concede acesso ao
  Key Vault.

Para leitura de credenciais, a atribuição recomendada é `Key Vault Secrets
User` no menor escopo praticável. Essa role lê o conteúdo de secrets, mas não os
administra. Consulte [Azure Key Vault RBAC](https://learn.microsoft.com/en-us/azure/key-vault/general/rbac-guide).

## Opção recomendada: AKS Workload Identity

Cada pod autentica de maneira independente:

```text
ServiceAccount Kubernetes
  -> token OIDC projetado no pod
  -> troca no Microsoft Entra ID
  -> access token curto para Key Vault
  -> SecretClient.get_secret()
  -> credencial mantida apenas em memória
```

Não existe uma credencial transferida do Airflow para o driver nem do driver
para os workers. Para cada ServiceAccount usada pelo motor, a infraestrutura
precisa:

1. habilitar OIDC issuer e Workload Identity no AKS;
2. criar ou selecionar uma User Assigned Managed Identity;
3. criar uma Federated Identity Credential cujo `subject` contenha namespace e
   nome exatos da ServiceAccount;
4. anotar a ServiceAccount com `azure.workload.identity/client-id`;
5. adicionar `azure.workload.identity/use: "true"` ao template do pod;
6. atribuir `Key Vault Secrets User` à identidade;
7. liberar DNS e tráfego para Entra e para o endpoint do Key Vault;
8. configurar Private Endpoint e Private DNS quando o acesso público estiver
   desabilitado.

O `AdaptiveIngestionOperator` adiciona o label ao Driver Job e grava
`_orchestration.workload_identity=true`. O construtor do Indexed Job lê essa
flag e adiciona o mesmo label aos workers. Essa é a **propagação** implementada:
propaga-se a política no template, não um token ou client secret.

O webhook do AKS injeta em cada pod as variáveis e o volume com seu próprio
token projetado. Driver e workers podem usar `WorkloadIdentityCredential` ou
`DefaultAzureCredential`. Veja [Microsoft Entra Workload ID no AKS](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview).

### Estado atual

O repositório já implementa a geração dos labels para driver e workers e evita
os Kubernetes Secrets padrão quando `credential_mode="workload_identity"`.
Ainda estão pendentes:

- `SecretResolver` baseado em `azure-identity` e `azure-keyvault-secrets`;
- `ArtifactStore` ADLS;
- ServiceAccounts produtivas anotadas;
- OIDC, Federated Identity Credentials, Azure RBAC e rede no ambiente AKS;
- um E2E contra um Key Vault real.

Portanto, selecionar `workload_identity` hoje valida o contrato Kubernetes, mas
não habilita sozinho o acesso ao Key Vault.

## Alternativa: Service Principal com client secret

Uma Service Principal pode autenticar sem OIDC, Federated Identity Credential,
webhook ou anotação de ServiceAccount. O SDK reconhece:

```text
AZURE_TENANT_ID
AZURE_CLIENT_ID
AZURE_CLIENT_SECRET
```

Com essas variáveis, `EnvironmentCredential`, `ClientSecretCredential` ou
`DefaultAzureCredential` obtém o token do Entra. Veja [Azure Identity para
Python](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python).

Isso torna a autenticação transparente para `SecretClient`, mas não torna a
operação inteira transparente para AKS. Continuam necessários:

| Responsabilidade | Necessária | Onde é configurada |
|---|---:|---|
| Role `Key Vault Secrets User` | sim | Azure RBAC |
| Rota e DNS para Microsoft Entra | sim | VNet, DNS e NetworkPolicy |
| Rota e DNS para Key Vault | sim | VNet, Private Endpoint/DNS e NetworkPolicy |
| Kubernetes RBAC de Jobs/pods/logs | sim | namespace do AKS |
| OIDC issuer e Workload Identity | não | dispensados nesse modo |
| Federated Identity Credential | não | dispensada nesse modo |
| Entrega e rotação do client secret | sim | plataforma de secrets |

### Formas de entrega

**Valor literal no operador ou no manifesto:** não recomendado. O segredo pode
ser persistido no metadata database do Airflow, na task renderizada, nos logs,
na API Kubernetes e no etcd. O Airflow também passa a atuar como secret broker.

**Referência a Kubernetes Secret:** aceitável como transição. A DAG fornece
somente `{name, secret, key}`. Driver e workers referenciam o mesmo Secret com
`valueFrom.secretKeyRef`; o kubelet materializa as variáveis no pod. O driver
não precisa de permissão Kubernetes `get` no recurso Secret. É preciso proteger
etcd, restringir leitura de Secrets, automatizar rotação e impedir logs de
ambiente.

**Secrets Store CSI Driver:** útil para certificados, Oracle Wallet e arquivos
que bibliotecas nativas precisam montar. Para username/password simples, o SDK
direto com Workload Identity reduz materialização em disco e configuração por
profile.

Se o Airflow criar um Kubernetes Secret temporário com o client secret, sua
ServiceAccount precisará de `create/delete` em Secrets, o que amplia o impacto
de um comprometimento. O desenho preferido é provisionar a identidade fora da
DAG e deixar Airflow criar apenas o Driver Job.

## Driver e workers

As duas categorias de pod podem consultar o Key Vault. O desenho recomendado é
usar identidades separadas quando suas permissões diferirem:

- o driver resolve somente o necessário para discovery, planejamento e
  publicação;
- os workers resolvem somente a credencial da origem e do destino necessários
  ao chunk;
- a configuração contém apenas a referência lógica ao segredo e, quando
  necessário, sua versão;
- nenhum valor resolvido entra em `plan.json`, `manifest.json`, métricas ou
  logs.

Compartilhar uma identidade entre driver e workers simplifica o primeiro
release, mas aumenta o conjunto de permissões de cada pod. A separação deve ser
adotada quando houver múltiplas origens, tenants ou domínios de dados.

## Matriz de decisão

| Critério | Workload Identity | Service Principal secret |
|---|---|---|
| Segredo permanente no cluster | não | sim |
| Preparação do AKS | OIDC, webhook, SA e FIC | rede e Kubernetes RBAC |
| Rotação | tokens automáticos | rotação do client secret |
| Exposição em Airflow | nenhuma | possível se houver injeção literal |
| Identidade por workload | nativa por ServiceAccount | exige secrets/configuração separados |
| Uso recomendado | produção | laboratório ou migração controlada |

## Critérios de aceite produtivo

- nenhum secret aparece em DAG serializada, Job, plano, log ou métrica;
- driver e workers autenticam depois de restart sem intervenção;
- revogar a atribuição Azure RBAC bloqueia novas leituras;
- NetworkPolicy permite apenas os endpoints necessários;
- Private DNS resolve o Key Vault para endereço privado quando aplicável;
- auditoria do Key Vault identifica a identidade e a versão lida;
- falhas distinguem autenticação, autorização, DNS, rede e secret inexistente;
- rotação não exige rebuild da imagem nem alteração da DAG.
