# DATABRICKS

**DELTA LIVE TABLE (DLT)**

🧩 Com a mudança recente agora é chamado de Lakeflow Declarative Pipelines

### CRIAÇÃO DE PIPELINE

> **CONCEITO**

É um framework projetado para simplificar a criação, execução e manutenção de pipelines.
Apenas declaramos no DLT as transformações e o framework cuida do resto.

✅ **Principais características**:

- [x] Dependência entre tabelas
- [x] Orquestrações dos jobs
- [x] Atualizações incrementais
- [x] Gerenciamento de cluster

✅ **Como funciona**:

Criamos um ou mais Pipelines (SQL) e podemos associar a um notebook. 

> **TABELAS**

Temos o schema **DSADLT** com duas tabelas:

- [x] dsa_fonte_clientes_diarios
- [x] dsa_mapeamento_clientes

Agora temos os dados disponíveis.

> **CONFIGURANDO O PIPELINE**

Temos varias fases para criar um **DLT**:

- [x] 1. General (Geral)
- [x] 2. Source Code
- [x] 3. Destination
- [x] 4. Notifiications
- [x] 5. Avanced

> **GERAL**

Em data Engineering clique em **Delta Live Tables** depois em CRIAR.

**Nome**: dsa-pipe-lab7
**Modo**: Triggered

🚨 Explicando o modos de execuções do Pipeline:

**Triggered**: É disparado apos a execução de uma ação.
**Continuous**: Fica ativado o tempo todo.

> **SOURCE CODE**

Informamos onde está o arquivo com o script SQL .

**Path**: 02-DSA-Delta-Live-Tables.sql

> **DESTINATION**

Informamos onde será o destino dos dados que serão gerados.

Em `catalogo` selecionamos **Workspace**.
Em `target schema` selecionado o schema **dsadlt**

> **NOTIFICATIONS**

☑️ Não precisa fazer nada.

> **ADVANCED**

☑️ Não precisa fazer nada.

🥊 Clique em CRIAR, pipeline configurado pronto para o uso.


### CÓDIGO SQL

Abrindo no DLT o pipeline **dsa-pipe-lab5** clique em **Source code**.

Agora podemos validar o pipeline clicando em **VALIDATE**.

> **RESULTADO DO VALIDATE**

Será gerado um grafo com as etapas de execuções do pipeline.
As 2 tabelas terão seus dados tratados e será gerado 2 views materializadas.

> **ENTENDO O CÓDIGO SQL**

🥉 **CRIANDO A CAMADA BRONZE**

| PROPRIEDADE | EXPLICAÇÃO |
| :--- | :--- |
|**CREATE LIVE TABLE** | Nome da tabela a ser criada.|
|**COMMENT** | Comentário|
|**TBLPROPERTIES** | Tipo de qualidade dos dados: 🥉 Bronze, 🥈 Silver e 🏅Gold.|

📌 A camada bronze contem os dados brutos de `dsa_mapeamento_tipo_cliente`.

```sql
-- Cria Bronze Live Table
CREATE LIVE TABLE dsa_mapeamento_tipo_cliente
COMMENT "Tabela Bronze para os tipos de clientes"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT *
FROM dsadlt.dsa_mapeamento_clientes;
```

| PROPRIEDADE | EXPLICAÇÃO |
| :--- | :--- |
|**CREATE OR REFRESH STREAMING TABLE**| Cria ou atualiza a tabela.|

Diferenças entre **LIVE TABLE** e **STREAMING TABLE**

▪️ **LIVE TABLE**
Usado para tabelas do tipo estática, onde os registro não mudam o tempo todo.

▪️ **STREAMING TABLE**
Usado para tabelas que recebe muitos dados, atualizou a origem ela atualiza.|

```sql
-- Cria Bronze Streaming Table
CREATE OR REFRESH STREAMING TABLE dsa_clientes_diarios
COMMENT "Tabela Bronze para os dados dos clientes"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT *
FROM STREAM(dsadlt.dsa_fonte_clientes_diarios);
```

🥈 **CRIANDO A CAMADA SILVER**

Geralmente essa camada é o resultado de uma limpeza e ou transformação das tabelas da camada bronze, neste exemplo faremos uma **consolidação dos dados** da camada bronze.

Incluindo restrição de qualidade, para isso usamos as **CONSTRAINT**.

**CONSTRAINT** - Demos o nome de **valid_data**
**EXPECT** - O que se esperar desses dados.
**ON VIOLATION** - Caso violar a regra remova a linha.

| **CAMPOS** | **VALIDAÇÕES** |
|:---|:---:|
| `id_cliente` | IS NOT NULL |
| `nome` | IS NOT NULL |
| `idade` | IS NOT NULL |
| `genero` | IS NOT NULL |
| `endereco` | IS NOT NULL |
| `numero_contato` | IS NOT NULL | 
| `data_cadastro` | IS NOT NULL |

No exemplo abaixo estamos fazendo um join entre as tabelas `dsa_clientes_diarios` e `dsa_mapeamento_tipo_cliente` pelo `tipo_cliente`.

```sql
-- Cria Silver Streaming Table
CREATE OR REFRESH STREAMING TABLE dsa_dados_limpos_clientes(CONSTRAINT valid_data EXPECT (id_cliente IS NOT NULL and `nome` IS NOT NULL and idade IS NOT NULL and genero IS NOT NULL and `endereco` IS NOT NULL and numero_contato IS NOT NULL and data_cadastro IS NOT NULL) ON VIOLATION DROP ROW)
COMMENT "Tabela Silver com dados de tabelas bronze e restrições de qualidade de dados"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
    p.id_cliente,
    p.nome,
    p.idade,
    p.genero,
    p.endereco,
    p.numero_contato,
    p.data_cadastro,
    m.desc_tipo
FROM STREAM(live.dsa_clientes_diarios) p
LEFT JOIN live.dsa_mapeamento_tipo_cliente m
ON p.tipo_cliente = m.codigo_tipo;
```

🥇 **CRIANDO A CAMADA GOLD**



```sql
-- Cria Gold Live Table 1
CREATE LIVE TABLE dsa_estatisticas_clientes
COMMENT "Tabela Gold com estatísticas gerais sobre os clientes"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    desc_tipo,
    COUNT(id_cliente) AS total_clientes,
    ROUND(AVG(idade), 2) AS media_idade,
    COUNT(DISTINCT genero) AS count_genero_distinct,
    MIN(idade) AS menor_idade,
    MAX(idade) AS maior_idade
FROM live.dsa_dados_limpos_clientes
GROUP BY desc_tipo;
```

```sql
-- Cria Gold Live Table 2
CREATE LIVE TABLE dsa_estatisticas_clientes_por_genero
COMMENT "Tabela Gold com estatísticas sobre os clientes com base no gênero"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
    genero,
    COUNT(id_cliente) AS total_clientes,
    ROUND(AVG(idade), 2) AS media_idade,
    MIN(idade) AS menor_idade,
    MAX(idade) AS maior_idade
FROM live.dsa_dados_limpos_clientes
GROUP BY genero;
```

