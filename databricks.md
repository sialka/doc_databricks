# DATABRICKS

## CRIANDO PIPELINE DE DADOS (DATALAKE HOUSE)

### ✅ 1. INGESTÃO DOS DADOS

Segue a documentação para criar um schema no catalago com volume para processar e gerar tabelas prontas para uso.

📎 **SCHEMA**

Para criar um schema, no menu catalog (catalago), em workspace clique em criar.

Nesse exemplo criamos um schema com o nome **dsasource**.

Sobre o schema, temos 3 tipos:

- volume
- table
- model

> VOLUME

Serve para armazenar dados de forma persistente.
Compartilhar dados entre usuários e processos.
Organizar dados fora de tabelas.

> TABLE

Serve para armazenar dados estruturados para consultas e processamentos via SQL ou Spark.

> MODEL

É um objeto que representa um modelo treinado, disponivel para ser reutilizado em previsões.

📎 **VOLUME**

Dentro do schema **dsasource**, clicamos em criar e escolhemos Volume.

Nome do volume: lab7
Tipo do volume: gerenciado

O volume pode ser:

- Gerenciado: 

É um repositório onde os arquivos estão armazenados, o databricks se encarrega de gerenciar o local físico dos arquivos.

- Externo

É um repositório onde o controle não fica sob responsabilidade do databricks, geralmente um bucket no S3.


📎 **UPLOAD**

Para inserir os dados dentro do volume fizemos o upload de arquivos .csv

Dentro do schema dsasource, dentro do volume lab7, clicamos em fazer upload e selecionamos os arquivos .csv localmente.

Arquivos:

- clientes_batch_01.csv
- clientes_batch_02.csv
- tipos_clientes.csv

Dentro de Workspace (Espaço de trabalho), dentro de Home (Início) clique no metro 3 pontos e escolha importar.

Arquivos:

- 01-DSA-Carga-Inicial_Dados.ipynb
- 02-DSA-Delta-Live-Tables.sql


📎 **NOTEBOOK**

Dentro do Workspace clicando sobre o arquivo **01-DSA-Carga-Inicial_Dados.ipynb** o Databricks já reconhece o arquivo e abre um notebook.

Para executar as celulas do notebook é necessário habilitar ou conectar um Serveless.
Para conectar clique no combox superior com o nome Serveless escolha uma máquina disponivel.


> Como descobrir o path dos arquivos csv

Em lab7 (volume) que está dentro de dsasource, no final da linha onde temos os arquivos clique nos 3 pontos e depois copiar caminho.

```bash
/Volumes/workspace/dsasource/lab7/clientes_batch_01.csv
/Volumes/workspace/dsasource/lab7/clientes_batch_02.csv
/Volumes/workspace/dsasource/lab7/tipos_clientes.csv
```

💡 **SCHEMA E TABELAS**

> 1. CRIANDO UM DF APARTIR DO .CSV

```python
df_dsa_tipos = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/Volumes/workspace/dsasource/lab7/tipos_clientes.csv")
````

Exibindo conteúdo

```python
display(df_dsa_tipos)
```

> 2. CRIANDO UM NOVO SCHEMA

```python
CREATE SCHEMA IF NOT EXISTS dsadlt;
```

> 3. CRIANDO TABELA DELTA 

Criando um tabela delta, a partir de um df.

```python
df_dsa_tipos.write.format("delta").mode("append").saveAsTable("dsadlt.dsa_mapeamento_clientes")
```

> 4. CRIANDO TABELA A PARTIR DE 2 CSV

Criando um unico DF Lendo os arquivos clientes_batch_*.csv

```python
df_dsa_clientes = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/Volumes/workspace/dsasource/lab7/clientes_batch_*.csv")
```

Ordenando o df pelo id_cliente

```python
df_ordenado = df_dsa_clientes.orderBy("id_cliente")
display(df_ordenado)
```

Ajustando o tipo da coluna de data.

```python
from pyspark.sql.functions import expr
df_dsa_clientes = df_dsa_clientes.withColumn("data_cadastro", expr("try_cast(data_cadastro as date)"))
```

Salva os dados na tabela.

```python
df_dsa_clientes.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("dsadlt.dsa_fonte_clientes_diarios")
```

> 5. COMO DELETAR TABELAS

```sql
%sql 
TRUNCATE TABLE dsadlt.dsa_mapeamento_clientes;
DROP TABLE dsadlt.dsa_mapeamento_clientes;

TRUNCATE TABLE dsadlt.dsa_clientes_diarios;
DROP TABLE dsadlt.dsa_clientes_diarios;
```
