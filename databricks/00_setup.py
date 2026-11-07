# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do lakehouse Boca de Lobo
# MAGIC
# MAGIC Rode **uma vez**, antes de subir o primeiro lote. Cria:
# MAGIC
# MAGIC | Objeto | Para quê |
# MAGIC |---|---|
# MAGIC | schema `workspace.gs` | todas as tabelas Bronze, Prata e Ouro |
# MAGIC | volume `workspace.gs.chegada` | onde os arquivos da pasta `chegada/` do Airflow são enviados pela interface |
# MAGIC | pastas `chegada/<fonte>/` | uma por fonte, porque o Auto Loader lê cada fonte da sua pasta |
# MAGIC | tabela `config_lgpd` | o **sal** dos hashes da Ouro. Fica no Unity Catalog, nunca no código ou no Git |
# MAGIC
# MAGIC Se a equipe preferir, o sal pode ir para um *secret scope* (`databricks secrets create-scope gs`
# MAGIC e `databricks secrets put-secret gs sal_lgpd`). O notebook 03 tenta o secret primeiro.

# COMMAND ----------

CATALOGO, SCHEMA = "workspace", "gs"
FONTES = ["previsao", "pluviometros", "areas_risco", "sp156", "inspecoes"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA} COMMENT 'GS 2026.2 - Boca de Lobo'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.chegada COMMENT 'Pouso dos arquivos coletados pelo Airflow'")

base = f"/Volumes/{CATALOGO}/{SCHEMA}/chegada"
for fonte in FONTES + ["_controle"]:
    dbutils.fs.mkdirs(f"{base}/{fonte}")
display(dbutils.fs.ls(base))

# COMMAND ----------

# Sal aleatório gerado UMA vez dentro do Databricks (não aparece no repositório).
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA}.config_lgpd (
  chave STRING, valor STRING, criado_em TIMESTAMP
) COMMENT 'Configuração sensível (sal dos hashes LGPD). Acesso restrito à equipe de dados.'
""")
if spark.table(f"{CATALOGO}.{SCHEMA}.config_lgpd").filter("chave = 'sal_lgpd'").count() == 0:
    spark.sql(f"""
      INSERT INTO {CATALOGO}.{SCHEMA}.config_lgpd
      SELECT 'sal_lgpd', sha2(concat(uuid(), uuid(), cast(rand() AS STRING)), 256), current_timestamp()
    """)
print("Sal LGPD pronto (valor não é exibido).")

# COMMAND ----------

# Tabelas de controle usadas pelos notebooks 01 e 02 (contagens e regras de qualidade)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA}.controle_cargas (
  executado_em TIMESTAMP, camada STRING, tabela STRING,
  linhas_antes BIGINT, linhas_depois BIGINT, linhas_inseridas BIGINT,
  arquivos_antes BIGINT, arquivos_depois BIGINT, observacao STRING
)""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA}.qualidade_regras (
  executado_em TIMESTAMP, tabela STRING, regra STRING, descricao STRING,
  registros_no_lote BIGINT, barrados BIGINT
)""")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOGO}.{SCHEMA}.prata_quarentena (
  executado_em TIMESTAMP, tabela STRING, regras_violadas ARRAY<STRING>,
  registro STRING, arquivo_origem STRING
)""")
print("Setup concluído. Agora envie o 1º lote ao volume e rode o notebook 01.")
