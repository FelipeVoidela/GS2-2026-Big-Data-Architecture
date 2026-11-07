# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — ingestão incremental com Auto Loader
# MAGIC
# MAGIC **Requisito 05.** Lê `/Volumes/workspace/gs/chegada/<fonte>/` com Auto Loader (`cloudFiles`) e grava
# MAGIC o dado **bruto** em `workspace.gs.bronze_<fonte>`, com três colunas de linhagem:
# MAGIC `arquivo_origem`, `data_coleta` (tirada do nome do arquivo) e `carregado_em`.
# MAGIC
# MAGIC Por que não duplica:
# MAGIC * o **checkpoint** de cada fonte guarda quais arquivos já foram lidos. No 2º lote, só os arquivos novos entram;
# MAGIC * rodar o notebook de novo sem arquivo novo insere **0 linhas** (a contagem abaixo prova isso);
# MAGIC * `trigger(availableNow=True)`: processa o que há e para (é o único modo do serverless).
# MAGIC
# MAGIC Tudo é lido como texto (`inferColumnTypes = false`): a Bronze guarda o que chegou, e a tipagem fica na Prata.
# MAGIC
# MAGIC **Como rodar para o relatório (item 07):** rode depois do lote 1, depois do lote 2 e mais uma vez sem lote novo.
# MAGIC A última célula mostra as três contagens.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO, SCHEMA = "workspace", "gs"
BASE = f"/Volumes/{CATALOGO}/{SCHEMA}/chegada"
CONTROLE = f"{BASE}/_controle"

FONTES = {
    # fonte: (formato, extensão, opções do leitor)
    "previsao":     ("json", "json", {"multiLine": "true"}),          # array JSON: 1 linha por ponto da grade
    "pluviometros": ("json", "json", {"multiLine": "true"}),          # array JSON: 1 linha por estação
    "areas_risco":  ("json", "json", {}),                             # JSON Lines: 1 linha por setor de risco
    "sp156":        ("csv",  "csv",  {"header": "true", "multiLine": "true", "escape": '"'}),
    "inspecoes":    ("json", "json", {}),                             # JSON Lines: 1 linha por inspeção do robô
}


def tabela(fonte: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.bronze_{fonte}"


def contagem(fonte: str) -> tuple[int, int]:
    if not spark.catalog.tableExists(tabela(fonte)):
        return 0, 0
    r = spark.table(tabela(fonte)).agg(F.count("*").alias("l"), F.countDistinct("arquivo_origem").alias("a")).first()
    return r["l"], r["a"]

# COMMAND ----------

antes = {f: contagem(f) for f in FONTES}

for fonte, (formato, ext, opcoes) in FONTES.items():
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{CONTROLE}/schema_{fonte}")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")   # campo novo inesperado vai para _rescued_data
        .option("pathGlobFilter", f"{fonte}_*.{ext}")
    )
    for chave, valor in opcoes.items():
        leitor = leitor.option(chave, valor)

    consulta = (
        leitor.load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("data_coleta", F.to_date(F.regexp_extract(F.col("_metadata.file_name"), r"(\d{4}-\d{2}-\d{2})", 1)))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{CONTROLE}/checkpoint_{fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(tabela(fonte))
    )
    consulta.awaitTermination()
    print(f"bronze_{fonte}: ok")

# COMMAND ----------

# Contagens antes/depois desta execução, gravadas em controle_cargas (histórico para o relatório)
linhas = []
for fonte in FONTES:
    (l0, a0), (l1, a1) = antes[fonte], contagem(fonte)
    linhas.append((f"bronze_{fonte}", l0, l1, l1 - l0, a0, a1))

df = spark.createDataFrame(linhas, "tabela string, linhas_antes long, linhas_depois long, linhas_inseridas long, arquivos_antes long, arquivos_depois long")
(df.select(F.current_timestamp().alias("executado_em"), F.lit("bronze").alias("camada"), "tabela",
           "linhas_antes", "linhas_depois", "linhas_inseridas", "arquivos_antes", "arquivos_depois",
           F.when(F.col("linhas_inseridas") == 0, "nenhum arquivo novo: nada duplicado")
            .otherwise("arquivos novos carregados").alias("observacao"))
   .write.mode("append").saveAsTable(f"{CATALOGO}.{SCHEMA}.controle_cargas"))
display(df)

# COMMAND ----------

# MAGIC %md ### Histórico das cargas (print para o item 07 do relatório)

# COMMAND ----------

display(spark.sql(f"""
  SELECT date_format(executado_em, 'dd/MM HH:mm:ss') AS execucao, tabela, linhas_antes, linhas_inseridas,
         linhas_depois, arquivos_antes, arquivos_depois, observacao
  FROM {CATALOGO}.{SCHEMA}.controle_cargas
  WHERE camada = 'bronze'
  ORDER BY executado_em, tabela
"""))

# COMMAND ----------

# Prova de que não há duplicata: cada arquivo aparece uma vez, com as suas linhas
display(spark.sql(f"""
  SELECT 'bronze_inspecoes' AS tabela, data_coleta, count(DISTINCT arquivo_origem) AS arquivos, count(*) AS linhas
  FROM {CATALOGO}.{SCHEMA}.bronze_inspecoes GROUP BY data_coleta
  UNION ALL
  SELECT 'bronze_previsao', data_coleta, count(DISTINCT arquivo_origem), count(*)
  FROM {CATALOGO}.{SCHEMA}.bronze_previsao GROUP BY data_coleta
  ORDER BY tabela, data_coleta
"""))
