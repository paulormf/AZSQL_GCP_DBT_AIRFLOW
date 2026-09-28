# Entrevista Porto — Data Engineering Pipeline

Pipeline de Engenharia de Dados desenvolvido para demonstrar práticas de ingestão incremental, Data Lake, Data Warehouse, modelagem dimensional, transformação com dbt, orquestração com Airflow, qualidade de dados e governança.

> **Status:** projeto funcional e executável localmente
> **Dados:** ambiente de treinamento com dados sintéticos

---

## Visão geral

O projeto implementa um pipeline completo de dados partindo de um banco relacional SQL Server até uma camada analítica preparada para consumo de BI.

O fluxo principal é:

```text
SQL Server
    ↓
Python
    ↓
GCS — Raw Data Lake
    ↓
BigQuery — Bronze
    ↓
dbt — Staging
    ↓
dbt — Silver
    ↓
dbt — Gold
    ↓
BI / Analytics
```

A solução foi construída com foco em:

* processamento incremental;
* tratamento de inserts e updates;
* controle de watermark;
* reprocessamento;
* particionamento;
* qualidade e integridade dos dados;
* modelagem dimensional;
* governança;
* observabilidade básica;
* orquestração com Airflow.

---

## Arquitetura

```text
┌──────────────────────┐
│      SQL Server      │
│      Fonte OLTP      │
└──────────┬───────────┘
           │
           │ Python / SQLAlchemy / pyodbc
           ▼
┌──────────────────────┐
│      GCS Raw         │
│      Data Lake       │
└──────────┬───────────┘
           │
           │ Load
           ▼
┌──────────────────────┐
│ BigQuery - Bronze    │
│ Dados brutos         │
└──────────┬───────────┘
           │
           │ dbt
           ▼
┌──────────────────────┐
│      Staging         │
│ Padronização         │
└──────────┬───────────┘
           │
           │ dbt
           ▼
┌──────────────────────┐
│       Silver         │
│ Entidades e fatos    │
└──────────┬───────────┘
           │
           │ dbt
           ▼
┌──────────────────────┐
│        Gold          │
│ Camada analítica     │
└──────────┬───────────┘
           │
           ▼
      BI / Analytics
```

---

## Stack tecnológica

| Categoria             | Tecnologia                   |
| --------------------- | ---------------------------- |
| Fonte                 | Microsoft SQL Server         |
| Linguagem             | Python                       |
| Ingestão              | Python + SQLAlchemy + pyodbc |
| Data Lake             | Google Cloud Storage         |
| Data Warehouse        | Google BigQuery              |
| Transformação         | dbt Core                     |
| Orquestração          | Apache Airflow               |
| Banco do Airflow      | PostgreSQL                   |
| Containerização       | Docker                       |
| Cloud                 | Google Cloud Platform        |
| Controle de versão    | Git                          |
| Qualidade             | dbt tests                    |
| Formato intermediário | Parquet                      |

---

## Fluxo de dados

### 1. Extração

A aplicação Python identifica as tabelas disponíveis na origem e valida a estrutura esperada.

Para tabelas novas ou sem dados anteriores no Bronze, é realizado carregamento inicial.

Para tabelas já processadas, a extração utiliza `updated_at` para identificar registros novos ou modificados.

Exemplo conceitual:

```sql
WHERE updated_at > :watermark
```

---

### 2. Data Lake

Os dados extraídos são armazenados em formato Parquet no Google Cloud Storage.

Exemplo de organização:

```text
gs://entrevistaporto-data-lake/

raw/
├── full/
│   └── <tabela>/
│       └── ...
└── incremental/
    └── <tabela>/
        └── YYYY/MM/DD/
            └── ...
```

O uso de Parquet permite armazenar os dados de forma eficiente e desacoplar a extração da etapa de processamento no Data Warehouse.

---

## Estratégia incremental

Um dos principais objetivos do projeto foi implementar uma estratégia de ingestão incremental que suporte tanto:

* novos registros;
* alterações em registros existentes.

Para isso, a origem possui a coluna:

```text
updated_at
```

O pipeline mantém uma tabela de controle no BigQuery:

```text
entrevistaporto_control.pipeline_watermark
```

Estrutura:

```text
table_name
watermark
watermark_source
```

O fluxo é:

```text
Último watermark
       ↓
SQL Server
       ↓
updated_at > watermark
       ↓
Novos / alterados
       ↓
GCS
       ↓
BigQuery staging
       ↓
MERGE
       ↓
BigQuery Bronze
       ↓
Novo watermark
```

### MERGE

O carregamento incremental utiliza a chave primária da tabela para diferenciar registros novos de registros existentes.

Conceitualmente:

```sql
MERGE INTO target AS t
USING staging AS s
ON t.id = s.id

WHEN MATCHED THEN
    UPDATE SET ...

WHEN NOT MATCHED THEN
    INSERT (...)
    VALUES (...);
```

Dessa forma, o pipeline não precisa recarregar toda a tabela a cada execução.

---

## Tratamento de precisão temporal

Um ponto importante da implementação foi preservar a precisão do `updated_at` proveniente do SQL Server.

A origem utiliza `datetime2(7)`.

O pipeline preserva o valor original em `watermark_source` e utiliza o valor convertido para controle no BigQuery.

Isso reduz o risco de perder registros quando múltiplas alterações ocorrem em intervalos muito pequenos.

---

## Lookback no dbt

O modelo incremental de pedidos utiliza uma janela de segurança de três dias:

```sql
updated_at >= timestamp_sub(
    max(updated_at),
    interval 3 day
)
```

O objetivo é proteger o processamento contra situações como:

* dados chegando atrasados;
* alterações fora da ordem esperada;
* pequenas diferenças de timestamp;
* reprocessamentos.

A contrapartida é o aumento da quantidade de registros avaliados.

Essa decisão representa um trade-off entre **robustez e custo de processamento**.

---

## Arquitetura Medallion

### Bronze

Responsável por manter os dados próximos da origem.

Características:

* dados brutos;
* rastreabilidade;
* possibilidade de replay;
* baixa transformação;
* acesso restrito.

Dataset:

```text
entrevistaporto_bronze
```

---

### Staging

Camada responsável pela padronização inicial.

Exemplos:

* conversão de tipos;
* `TRIM`;
* normalização de texto;
* normalização de estados;
* conversão para `TIMESTAMP`;
* padronização de nomes.

Exemplo:

```sql
upper(trim(estado)) as estado
```

Dataset:

```text
entrevistaporto_staging
```

---

### Silver

Camada de dados curados e reutilizáveis.

Contém entidades e fatos utilizados por diferentes consumidores.

Exemplos:

```text
dim_clientes
dim_vendedores
dim_produtos
fct_pedidos
fct_itens_pedido
```

Também são aplicadas regras como hash de informações sensíveis.

Exemplo:

```sql
to_hex(sha256(cast(email as bytes))) as email_hash
```

---

### Gold

Camada orientada ao consumo analítico e BI.

Principais modelos:

```text
mart_vendas
mart_vendas_itens
```

A camada Gold combina fatos e dimensões para disponibilizar dados em uma estrutura mais simples para consumo.

---

## Modelagem dimensional

A solução utiliza uma abordagem de Star Schema.

```text
                    dim_clientes
                         │
                         │
dim_vendedores ──── fct_pedidos
                         │
                         │ id_pedido
                         │
                  fct_itens_pedido
                         │
                         │
                    dim_produtos
```

### Granularidade

`fct_pedidos`:

> Uma linha representa um pedido.

`fct_itens_pedido`:

> Uma linha representa um item de pedido.

Definir explicitamente a granularidade evita ambiguidades na construção das métricas analíticas.

---

## Particionamento e clustering

A tabela de pedidos utiliza particionamento por:

```text
data_pedido
```

com granularidade diária.

Também utiliza clustering por:

```text
id_cliente
id_vendedor
```

O objetivo é reduzir a quantidade de dados processados em consultas analíticas que utilizam filtros temporais e dimensões frequentemente utilizadas.

O particionamento físico é tratado separadamente da lógica de processamento incremental.

---

## Orquestração com Airflow

O Airflow coordena o pipeline completo.

DAG:

```text
entrevistaporto_pipeline
```

Fluxo:

```text
validate_sources
       ↓
extract_and_load_bronze
       ↓
dbt_build
       ↓
quality_checks
```

A validação inicial verifica a disponibilidade e estrutura das fontes antes da execução do pipeline.

O processamento de dados é executado posteriormente, seguido da transformação com dbt e dos testes de qualidade.

---

## Qualidade de dados

Foram implementados testes utilizando dbt.

Principais categorias:

### Not Null

Verifica campos obrigatórios.

```text
id_cliente
id_pedido
id_vendedor
```

### Unique

Valida unicidade das chaves.

```text
id_cliente
id_pedido
id_produto
id_item
```

### Relationships

Valida integridade referencial entre fatos e dimensões.

Exemplo:

```text
fct_itens_pedido.id_pedido
        ↓
fct_pedidos.id_pedido
```

### Regras de negócio

Também são utilizadas validações específicas quando aplicável.

Resultado atual do projeto:

```text
70 operações
PASS = 70
WARN = 0
ERROR = 0
SKIP = 0
```

---

## Governança e segurança

O projeto possui uma classificação de dados para orientar o tratamento das informações.

Exemplos de dados que exigem maior controle:

```text
Clientes
├── nome
└── email

Vendedores
└── nome

Pedidos
└── valor_total

Itens
├── preco_unitario
└── desconto
```

Informações sensíveis são tratadas de acordo com sua finalidade.

Por exemplo, o e-mail dos clientes é transformado em hash na camada Silver:

```sql
SHA256(email)
```

Credenciais não fazem parte do repositório.

São utilizadas variáveis de ambiente para:

```text
AZURE_SQL_USERNAME
AZURE_SQL_PASSWORD
GCP_PROJECT_ID
AIRFLOW_ADMIN_PASSWORD
AIRFLOW_JWT_SECRET
POSTGRES_PASSWORD
```

A chave de serviço do Google Cloud também permanece fora do Git:

```text
keys/gcp.json
```

---

## Resiliência da conexão

A comunicação com o SQL Server possui mecanismos de proteção contra falhas transitórias.

O SQLAlchemy utiliza:

```text
pool_pre_ping
pool_recycle
```

Além disso, operações SQL possuem retry para erros de conexão transitórios.

Exemplos tratados:

```text
08001
08S01
HYT00
HYT01
connection reset
timeout expired
```

As tentativas utilizam backoff:

```text
5 segundos
15 segundos
30 segundos
```

Isso evita que uma falha momentânea de conectividade interrompa imediatamente toda a execução do pipeline.

---

## Estrutura do projeto

```text
entrevistaporto/
│
├── airflow/
│   ├── config/
│   │   └── profiles.yml
│   ├── dags/
│   │   └── entrevistaporto_pipeline.py
│   ├── docker/
│   │   └── Dockerfile
│   └── docker-compose.yml
│
├── entrevistaporto_dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
│
├── governance/
│   └── data_classification.yml
│
├── Scripts sql/
│
├── src/
│   ├── discover_tables.py
│   ├── extract_all.py
│   ├── load_bronze.py
│   ├── pipeline_incremental.py
│   ├── upload_gcs.py
│   ├── validate_load.py
│   └── validate_raw.py
│
├── .env.example
├── .gitignore
├── pyproject.toml
├── README.md
└── uv.lock
```

---

## Como executar

### Pré-requisitos

* Python 3.14+
* Docker Desktop
* Docker Compose
* Google Cloud Storage
* Google BigQuery
* acesso a uma instância SQL Server
* credencial de serviço do Google Cloud

---

### Configuração

Copie o arquivo de exemplo:

```powershell
Copy-Item .env.example .env
```

Configure as variáveis necessárias no `.env`.

A credencial do Google Cloud deve estar em:

```text
keys/gcp.json
```

Esse arquivo não deve ser versionado.

---

### Ambiente Python

Instale as dependências utilizando `uv`:

```powershell
uv sync
```

Ative o ambiente virtual:

```powershell
.venv\Scripts\Activate.ps1
```

---

### Testar o dbt localmente

```powershell
cd entrevistaporto_dbt

dbt debug
```

Executar modelos e testes:

```powershell
dbt build
```

---

### Executar Airflow

A partir da raiz do projeto:

```powershell
docker compose --env-file .env -f airflow\docker-compose.yml up -d
```

Verificar os serviços:

```powershell
docker compose --env-file .env -f airflow\docker-compose.yml ps
```

O Airflow estará disponível em:

```text
http://localhost:8081
```

---

### Validar o dbt dentro do Airflow

```powershell
docker compose --env-file .env -f airflow\docker-compose.yml exec airflow-scheduler dbt debug --project-dir /opt/airflow/project/entrevistaporto_dbt --profiles-dir /opt/airflow/project/config
```

---

## Decisões técnicas

### Por que GCS antes do BigQuery?

O Data Lake desacopla a extração da carga no Data Warehouse.

Isso permite:

* armazenamento dos dados brutos;
* possibilidade de replay;
* auditoria;
* reprocessamento;
* desacoplamento entre ingestão e transformação.

### Por que Parquet?

Parquet oferece:

* armazenamento colunar;
* compressão eficiente;
* bom desempenho analítico;
* compatibilidade com diversas ferramentas de dados.

### Por que dbt?

O dbt concentra as transformações SQL e permite:

* versionamento;
* testes;
* documentação;
* dependências entre modelos;
* materializações;
* processamento incremental.

### Por que Airflow?

O Airflow permite transformar as etapas individuais em um workflow controlado, com dependências, retries, logs e execução programada.

---

## Desafios técnicos

Durante o desenvolvimento foram tratados alguns cenários próximos de problemas encontrados em ambientes reais:

* processamento incremental de inserts e updates;
* precisão de timestamps do SQL Server;
* dados atrasados;
* falhas transitórias de conexão;
* particionamento de tabelas;
* duplicidade em modelos incrementais;
* integridade referencial;
* separação entre dados brutos e dados analíticos;
* gerenciamento de credenciais;
* execução do dbt dentro de containers;
* comunicação entre componentes do Airflow 3.

Um exemplo importante foi a validação da estratégia incremental do dbt. Uma tentativa utilizando filtros no lado destino do `MERGE` poderia impedir que registros existentes fossem encontrados para atualização, causando duplicidade. O problema foi identificado através dos testes de unicidade e a estratégia foi ajustada.

---

## Limitações

Este projeto utiliza dados sintéticos e possui volume controlado.

Por isso, algumas decisões que seriam necessárias em um ambiente de produção de grande escala ainda não são aplicadas, como:

* processamento distribuído com Spark;
* catálogo de dados;
* gerenciamento corporativo de secrets;
* observabilidade centralizada;
* alertas integrados;
* CI/CD completo;
* infraestrutura como código;
* controle de acesso corporativo no BigQuery.

Esses pontos fazem parte das possíveis evoluções do projeto.

---

## Próximos passos

Como evolução do projeto, estão planejados:

* adicionar uma fonte NoSQL, como MongoDB;
* executar testes controlados com volumes maiores;
* avaliar estratégias de processamento distribuído;
* mapear a arquitetura para AWS;
* explorar Azure Data Lake / Lakehouse;
* adicionar CI/CD;
* ampliar observabilidade e monitoramento.

---

## Objetivo profissional

O projeto foi desenvolvido como laboratório prático para demonstrar conhecimentos aplicados de Engenharia de Dados, incluindo:

```text
SQL
Python
ETL / ELT
Data Lake
Data Warehouse
Cloud
dbt
Airflow
BigQuery
GCS
Modelagem dimensional
Data Quality
Governança
Git
Docker
```

A arquitetura prioriza separação de responsabilidades, processamento incremental, qualidade dos dados, segurança e facilidade de manutenção.
