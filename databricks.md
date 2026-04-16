# DATABRICKS

## CRIANDO PIPELINE DE DADOS (DATALAKE HOUSE)

### 1. INGESTÃO DOS DADOS

**CREATE SCHEMA**

Para criar um schema, no menu catalog (catalago), em workspace clique em criar.

Nesse exemplo criamos um schema com o nome dsasource

Os schema podem ser de 3 tipos:

- volume
- table
- model

> VOLUME

Serve para armazenar dados de forma persistente
Compartilhar dados entre usuários e processos
organizar dados fora de tabelas

> TABLE

Serve para armazenar dados estruturados para consultas e processamentos via SQL ou Spark.

> MODEL

É um objeto que representa um modelo treinado, disponivel para ser reutilizado em previsões.

**CREATE VOLUME**

O volume pode ser:

- Gerenciado: 

É um repositório onde os arquivos estão armazenados, o databricks se encarrega de gerenciar o local físico dos arquivos.

- Externo

É um repositório onde o controle não fica sob responsabilidade do databricks, geralmente um bucket no S3.

Neste exemplo criamos um volume para armazenar os dados.

Dentro do schema dsasource, clicamos em Criar e escolhemos Volume.

Nome do volume: lab7
Tipo do volume: gerenciado


**UPLOAD**

Para inserir os dados dentro do volume fizemos o upload de arquivos .csv

Dentro do schema dsasource, dentro do volume lab7, clicamos em fazer upload e selecionamos os arquivos .csv localmente.

Arquivos:

clientes_batch_01.csv
clientes_batch_02.csv
tipos_clientes.csv

Dentro de Workspace (Espaço de trabalho), dentro de Home (Início) clique no metro 3 pontos e escolha importar.

Arquivos:

01-DSA-Carga-Inicial_Dados.ipynb
02-DSA-Delta-Live-Tables.sql


**ABRINDO NOTEBOOK**

Dentro do Worspace clicando sobre o arquivo 01-DSA-Carga-Inicial_Dados.ipynb o Databricks já reconhece o arquivo e abre dentro de um notebook.

Para executar as celulas do notebook é necessário habilitar ou conectar um Serveless.
Para conectar clique no combox superior com o nome Serveless escolha uma máquina disponivel.

**NOVO SCHEMA COM TABELAS**

> Como descobrir o path dos arquivos csv

Em lab7 (volume) que está dentro de dsasource, no final da linha onde temos os arquivos clique nos 3 pontos e depois copiar caminho.

/Volumes/workspace/dsasource/lab7/clientes_batch_01.csv
/Volumes/workspace/dsasource/lab7/clientes_batch_02.csv
/Volumes/workspace/dsasource/lab7/tipos_clientes.csv

1. CRIANDO UM DF APARTIR DO .CSV

df_dsa_tipos = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/Volumes/workspace/dsasource/lab7/tipos_clientes.csv")

> Exibindo conteúdo

display(df_dsa_tipos)

2. CRIANDO UM NOVO SCHEMA

CREATE SCHEMA IF NOT EXISTS dsadlt;

3. INSERINDO UMA TABELA DENTRO DO NOVO SCHEMA

> Criando um tabela delta, a partir de um df.

df_dsa_tipos.write.format("delta").mode("append").saveAsTable("dsadlt.dsa_mapeamento_clientes")

> Criando um unico DF Lendo os arquivos clientes_batch_*.csv

df_dsa_clientes = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/Volumes/workspace/dsasource/lab7/clientes_batch_*.csv")

> Ordenando o df pelo id_cliente

df_ordenado = df_dsa_clientes.orderBy("id_cliente")
display(df_ordenado)

> Ajustando o tipo da coluna de data.

from pyspark.sql.functions import expr
df_dsa_clientes = df_dsa_clientes.withColumn("data_cadastro", expr("try_cast(data_cadastro as date)"))

> Salva os dados na tabela.

df_dsa_clientes.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("dsadlt.dsa_fonte_clientes_diarios")

> Caso precise deletar as tabelas

%sql 
TRUNCATE TABLE dsadlt.dsa_mapeamento_clientes;
DROP TABLE dsadlt.dsa_mapeamento_clientes;

TRUNCATE TABLE dsadlt.dsa_clientes_diarios;
DROP TABLE dsadlt.dsa_clientes_diarios;


