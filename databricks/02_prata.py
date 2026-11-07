# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata — tipos, nulos, inválidos, regras de qualidade e MERGE
# MAGIC
# MAGIC **Requisito 06.** Para cada fonte, a Bronze é lida em *streaming* (só o que entrou desde a última execução)
# MAGIC e cada micro-lote passa por quatro passos dentro de `foreachBatch`:
# MAGIC
# MAGIC 1. **Tipagem e tratamento:** `try_cast` / `try_to_timestamp` (o serverless roda em modo ANSI, e um `cast` comum
# MAGIC    quebraria com o `"-"` que o Cemaden manda), padronização de texto, fuso de São Paulo;
# MAGIC 2. **3 regras de qualidade por fonte:** cada registro é testado em todas; o total barrado **por regra** vai para
# MAGIC    `qualidade_regras`, e o registro barrado vai inteiro para `prata_quarentena` (nada some sem rastro);
# MAGIC 3. **Deduplicação** pela chave dentro do lote (o MERGE falharia com duas linhas para a mesma chave);
# MAGIC 4. **MERGE pela chave** na tabela Prata: atualiza se já existe, insere se é novo. Rodar de novo não duplica.
# MAGIC
# MAGIC | Fonte | Tabela Prata | Chave do MERGE |
# MAGIC |---|---|---|
# MAGIC | Open-Meteo | `prata_previsao_chuva` | `celula_id, data_hora, data_coleta` |
# MAGIC | Cemaden | `prata_pluviometro` | `id_estacao, data_hora_leitura` |
# MAGIC | GeoSampa | `prata_area_risco` (+ `prata_area_risco_h3`) | `id_area` |
# MAGIC | SP156 | `prata_solicitacao_sp156` | `id_solicitacao` (hash do registro: o arquivo aberto não traz protocolo) |
# MAGIC | Robô | `prata_inspecao` | `id_inspecao` |

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

CATALOGO, SCHEMA = "workspace", "gs"
T = lambda nome: f"{CATALOGO}.{SCHEMA}.{nome}"  # noqa: E731
CONTROLE = f"/Volumes/{CATALOGO}/{SCHEMA}/chegada/_controle"
LINHAGEM = ["arquivo_origem", "data_coleta", "carregado_em"]

# Limites geográficos do município de São Paulo (com folga)
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = -24.10, -23.20, -47.00, -46.20


def tc(col: str, tipo: str):
    """try_cast: devolve NULL em vez de quebrar quando o valor não converte."""
    return F.expr(f"try_cast({col} AS {tipo})")


def dentro_de_sp(lat, lon):
    return lat.between(LAT_MIN, LAT_MAX) & lon.between(LON_MIN, LON_MAX)


def js(col: str, caminho: str) -> str:
    """A Bronze guarda objetos JSON aninhados como texto (inferColumnTypes = false): extrai um campo pelo caminho."""
    return f"get_json_object({col}, '$.{caminho}')"

# COMMAND ----------

# MAGIC %md ## Motor genérico: regras → quarentena → dedup → MERGE

# COMMAND ----------

def processar(fonte: str, destino: str, transformar, regras: list, chave: list[str]) -> None:
    """regras: lista de (nome, descrição, condição_que_o_registro_VÁLIDO_cumpre)."""

    def lote(batch_df: DataFrame, batch_id: int) -> None:
        s = batch_df.sparkSession
        df = transformar(batch_df)
        colunas = df.columns

        violadas = F.filter(
            F.array(*[F.when(~F.coalesce(cond, F.lit(False)), F.lit(nome)) for nome, _, cond in regras]),
            lambda x: x.isNotNull(),
        )
        df = df.withColumn("_violadas", violadas)

        # 1) contagem por regra
        agg = df.agg(F.count("*").alias("_total"),
                     *[F.sum(F.array_contains("_violadas", nome).cast("int")).alias(nome) for nome, _, _ in regras]).first()
        total = agg["_total"]
        if total == 0:
            return
        linhas = [(destino, nome, desc, total, int(agg[nome] or 0)) for nome, desc, _ in regras]

        # 2) quarentena com o registro inteiro
        (df.filter(F.size("_violadas") > 0)
           .select(F.current_timestamp().alias("executado_em"), F.lit(destino).alias("tabela"),
                   F.col("_violadas").alias("regras_violadas"),
                   F.to_json(F.struct(*colunas)).alias("registro"), F.col("arquivo_origem"))
           .write.mode("append").saveAsTable(T("prata_quarentena")))

        # 3) dedup pela chave (fica a versão da coleta mais recente)
        validos = df.filter(F.size("_violadas") == 0).drop("_violadas")
        n_validos = validos.count()
        w = Window.partitionBy(*chave).orderBy(F.col("data_coleta").desc(), F.col("carregado_em").desc())
        validos = (validos.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
                          .withColumn("processado_em", F.current_timestamp()))
        n_unicos = validos.count()
        linhas.append((destino, "DEDUP", f"registros repetidos pela chave ({', '.join(chave)}) no lote",
                       total, n_validos - n_unicos))

        s.createDataFrame(linhas, "tabela string, regra string, descricao string, registros_no_lote long, barrados long") \
         .select(F.current_timestamp().alias("executado_em"), "*") \
         .write.mode("append").saveAsTable(T("qualidade_regras"))

        # 4) MERGE pela chave
        if not s.catalog.tableExists(T(destino)):
            validos.limit(0).write.saveAsTable(T(destino))
        validos.createOrReplaceTempView(f"lote_{destino}")
        cond = " AND ".join(f"alvo.{c} <=> novo.{c}" for c in chave)
        s.sql(f"""
            MERGE INTO {T(destino)} AS alvo
            USING lote_{destino} AS novo
            ON {cond}
            WHEN MATCHED AND novo.data_coleta >= alvo.data_coleta THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    (spark.readStream.table(T(f"bronze_{fonte}"))
          .writeStream.foreachBatch(lote)
          .option("checkpointLocation", f"{CONTROLE}/checkpoint_prata_{fonte}")
          .trigger(availableNow=True)
          .start()
          .awaitTermination())
    print(f"{destino}: ok")


# Texto formatado pelo próprio Spark (fuso da sessão): um datetime do Python perderia o fuso e o filtro erraria em 3 h.
inicio_execucao = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS') AS t").first()["t"]

# COMMAND ----------

# MAGIC %md ## 1. Open-Meteo → `prata_previsao_chuva`
# MAGIC O Open-Meteo devolve listas paralelas (`time[]`, `precipitation[]`, `precipitation_probability[]`).
# MAGIC `posexplode` abre uma linha por hora e usa a posição para pegar o valor certo nas outras listas
# MAGIC (mesmo efeito do `arrays_zip` + `explode`, sem depender do nome que o Spark dá aos campos do zip).

# COMMAND ----------

HOURLY = "struct<time:array<string>, precipitation:array<string>, precipitation_probability:array<string>>"


def t_previsao(df):
    return (
        df.withColumn("hourly", F.from_json("hourly", HOURLY))   # na Bronze o objeto hourly é texto JSON
        .select(
            tc("lat_grade", "DECIMAL(5,1)").alias("lat_grade"),
            tc("lon_grade", "DECIMAL(5,1)").alias("lon_grade"),
            tc("latitude", "DOUBLE").alias("lat_modelo"),
            tc("longitude", "DOUBLE").alias("lon_modelo"),
            tc("elevation", "DOUBLE").alias("elevacao_m"),
            F.posexplode("hourly.time").alias("i", "hora_txt"),
            F.col("hourly.precipitation").alias("_prec"),
            F.col("hourly.precipitation_probability").alias("_prob"),
            *LINHAGEM,
        )
        .withColumn("celula_id", F.concat_ws("_", F.col("lat_grade").cast("string"), F.col("lon_grade").cast("string")))
        .withColumn("data_hora", F.expr("try_to_timestamp(hora_txt, \"yyyy-MM-dd'T'HH:mm\")"))
        .withColumn("precipitacao_mm", F.expr("try_cast(try_element_at(_prec, i + 1) AS DOUBLE)"))
        .withColumn("prob_chuva_pct", F.expr("try_cast(try_element_at(_prob, i + 1) AS INT)"))
        .drop("i", "hora_txt", "_prec", "_prob")
    )


processar(
    "previsao", "prata_previsao_chuva", t_previsao,
    regras=[
        ("PREV_01_precipitacao_valida", "precipitação não nula e entre 0 e 200 mm/h",
         F.col("precipitacao_mm").between(0, 200)),
        ("PREV_02_probabilidade_valida", "probabilidade de chuva nula ou entre 0 e 100%",
         F.col("prob_chuva_pct").isNull() | F.col("prob_chuva_pct").between(0, 100)),
        ("PREV_03_celula_e_hora_validas", "ponto da grade dentro de SP e hora legível",
         dentro_de_sp(F.col("lat_grade"), F.col("lon_grade")) & F.col("data_hora").isNotNull()),
    ],
    chave=["celula_id", "data_hora", "data_coleta"],
)

# COMMAND ----------

# MAGIC %md ## 2. Cemaden → `prata_pluviometro`
# MAGIC A data da última leitura vem em UTC no formato `dd/MM/yy HH:mm`. Estações sem leitura mandam `"-"`.

# COMMAND ----------

def t_pluviometro(df):
    acc = {f"acc_{h}h": tc(f"acc{h}hr", "DOUBLE") for h in (1, 3, 6, 12, 24, 48, 72, 96)}
    return df.select(
        tc("idestacao", "INT").alias("id_estacao"),
        F.initcap(F.trim("nomeestacao")).alias("nome_estacao"),
        F.upper(F.trim("cidade")).alias("cidade"),
        tc("codibge", "INT").alias("cod_ibge"),
        F.col("uf"),
        F.expr("try_to_timestamp(concat(datahoraUltimovalor, ' +0000'), 'dd/MM/yy HH:mm Z')").alias("data_hora_leitura"),
        *[c.alias(n) for n, c in acc.items()],
        tc("tipoestacao", "INT").alias("tipo_estacao"),
        tc("status", "INT").alias("status_estacao"),
        *LINHAGEM,
    )


processar(
    "pluviometros", "prata_pluviometro", t_pluviometro,
    regras=[
        ("PLUV_01_leitura_numerica", "acumulado de 24 h numérico (estação não mandou '-')",
         F.col("acc_24h").isNotNull()),
        ("PLUV_02_leitura_recente", "última leitura com até 2 dias da coleta (estação não está parada)",
         F.col("data_hora_leitura") >= F.date_sub("data_coleta", 2)),
        ("PLUV_03_acumulados_coerentes", "acumulados não negativos e 96 h >= 24 h",
         (F.col("acc_24h") >= 0) & (F.col("acc_96h") >= F.col("acc_24h"))),
    ],
    chave=["id_estacao", "data_hora_leitura"],
)

# COMMAND ----------

# MAGIC %md ## 3. GeoSampa → `prata_area_risco` e `prata_area_risco_h3`

# COMMAND ----------

def t_area_risco(df):
    grau = F.upper(F.trim("tx_grau_risco_hidrologico"))
    processo = F.upper(F.trim(F.translate(F.col("tx_tipo_processo"), "ÇÃçã\n", "CAca ")))
    return df.select(
        tc("cd_identificador_risco_hidrologico", "INT").alias("id_area"),
        F.initcap(F.trim("nm_area_risco_hidrologico")).alias("nome_area"),
        F.trim("sg_area_risco_hidrologico").alias("sigla_area"),
        F.trim("sg_setor_risco_hidrologico").alias("setor"),
        grau.alias("grau_risco"),
        F.expr("try_cast(substr(upper(trim(tx_grau_risco_hidrologico)), 2) AS INT)").alias("grau_num"),
        F.regexp_replace(processo, "INUDACAO", "INUNDACAO").alias("tipo_processo"),
        tc("qt_moradia", "INT").alias("qt_moradias"),
        F.upper(F.trim("nm_subprefeitura")).alias("subprefeitura"),
        F.upper(F.trim("nm_bacia_hidrografica")).alias("bacia"),
        F.expr("to_date(try_to_timestamp(substr(dt_vistoria, 1, 10), 'yyyy-MM-dd'))").alias("data_vistoria"),
        F.col("geometria_geojson"),
        *LINHAGEM,
    )


processar(
    "areas_risco", "prata_area_risco", t_area_risco,
    regras=[
        ("AREA_01_grau_valido", "grau de risco entre R1 e R4", F.col("grau_risco").isin("R1", "R2", "R3", "R4")),
        ("AREA_02_geometria_valida", "polígono GeoJSON presente",
         F.get_json_object("geometria_geojson", "$.type").isin("Polygon", "MultiPolygon")),
        ("AREA_03_moradias_validas", "número de moradias informado e não negativo", F.col("qt_moradias") >= 0),
    ],
    chave=["id_area"],
)

# COMMAND ----------

# Células H3 (res. 10, ~66 m de lado) de cada área de risco: anel 0 = dentro, anel 1 = vizinha (~130 m).
# Assim o cruzamento bueiro x área de risco vira um JOIN por inteiro, sem geometria pesada.
spark.sql(f"""
CREATE OR REPLACE TABLE {T('prata_area_risco_h3')}
COMMENT 'Células H3 res 10 das áreas de risco hidrológico (anel 0 = dentro, 1 = vizinhança)'
AS
WITH base AS (
  SELECT id_area, grau_num,
         array_union(
           coalesce(h3_try_polyfillash3(geometria_geojson, 10), array()),
           transform(from_json(geometria_geojson, 'struct<coordinates:array<array<array<double>>>>').coordinates[0],
                     p -> h3_longlatash3(p[0], p[1], 10))
         ) AS celulas
  FROM {T('prata_area_risco')}
),
dentro AS (SELECT id_area, grau_num, explode(celulas) AS h3_10 FROM base),
com_vizinhos AS (
  SELECT id_area, grau_num, h3_10, 0 AS anel FROM dentro
  UNION ALL
  SELECT id_area, grau_num, viz AS h3_10, 1 AS anel FROM dentro LATERAL VIEW explode(h3_kring(h3_10, 1)) k AS viz
)
SELECT id_area, grau_num, h3_10, min(anel) AS anel
FROM com_vizinhos GROUP BY id_area, grau_num, h3_10
""")
display(spark.sql(f"SELECT anel, count(*) AS celulas, count(DISTINCT id_area) AS areas FROM {T('prata_area_risco_h3')} GROUP BY anel"))

# COMMAND ----------

# MAGIC %md ## 4. SP156 → `prata_solicitacao_sp156`
# MAGIC Datas no formato americano `MM/dd/yyyy`. O arquivo aberto não traz número de protocolo, então a chave é o
# MAGIC hash SHA-256 do registro inteiro: linhas idênticas viram uma só (contadas em `DEDUP`).

# COMMAND ----------

def t_sp156(df):
    campos = ["data_de_abertura", "data_de_finalizacao", "meio_de_comunicacao", "tema", "assunto", "servico",
              "status", "orgao", "logradouro", "numero", "cep", "latitude", "longitude",
              "prefeitura_operacional", "distrito"]
    return df.select(
        F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in campos]), 256).alias("id_solicitacao"),
        F.expr("to_date(try_to_timestamp(data_de_abertura, 'MM/dd/yyyy'))").alias("data_abertura"),
        F.expr("to_date(try_to_timestamp(data_de_finalizacao, 'MM/dd/yyyy'))").alias("data_finalizacao"),
        F.upper(F.trim("meio_de_comunicacao")).alias("canal"),
        F.trim("assunto").alias("assunto"),
        F.trim("servico").alias("servico"),
        F.trim("status").alias("status"),
        F.trim("orgao").alias("orgao"),
        F.initcap(F.trim("logradouro")).alias("logradouro"),
        F.trim("numero").alias("numero"),                     # dado sensível: só sai da Prata mascarado
        F.regexp_replace("cep", "[^0-9]", "").alias("cep"),   # idem
        tc("latitude", "DOUBLE").alias("latitude"),
        tc("longitude", "DOUBLE").alias("longitude"),
        F.upper(F.trim("prefeitura_operacional")).alias("subprefeitura"),
        F.upper(F.trim("distrito")).alias("distrito"),
        F.col("recurso_ckan"),
        *LINHAGEM,
    )


processar(
    "sp156", "prata_solicitacao_sp156", t_sp156,
    regras=[
        ("SP156_01_coordenada_no_municipio", "latitude/longitude presentes e dentro de São Paulo",
         dentro_de_sp(F.col("latitude"), F.col("longitude"))),
        ("SP156_02_abertura_valida", "data de abertura legível e não futura",
         F.col("data_abertura").isNotNull() & (F.col("data_abertura") <= F.col("data_coleta"))),
        ("SP156_03_finalizacao_coerente", "finalização vazia ou depois da abertura",
         F.col("data_finalizacao").isNull() | (F.col("data_finalizacao") >= F.col("data_abertura"))),
    ],
    chave=["id_solicitacao"],
)

# COMMAND ----------

# MAGIC %md ## 5. Robô → `prata_inspecao`
# MAGIC JSON aninhado (sensores, visão, IA embarcada, operador) chega da Bronze como texto e é achatado em colunas tipadas
# MAGIC com `get_json_object` + `try_cast`.

# COMMAND ----------

CLASSES_YOLO = ["livre", "parcialmente_obstruido", "entupido", "lixo_sarjeta"]


def t_inspecao(df):
    return df.select(
        F.col("id_inspecao"),
        F.col("id_bueiro"),
        F.col("robo_id"),
        F.expr("try_to_timestamp(inspecionado_em)").alias("inspecionado_em"),
        tc(js("localizacao", "lat"), "DOUBLE").alias("latitude"),          # objetos aninhados chegam como texto JSON
        tc(js("localizacao", "lon"), "DOUBLE").alias("longitude"),
        F.initcap(F.trim(F.expr(js("endereco", "logradouro")))).alias("logradouro"),
        F.upper(F.trim(F.expr(js("endereco", "distrito")))).alias("distrito"),
        F.upper(F.trim(F.expr(js("endereco", "subprefeitura")))).alias("subprefeitura"),
        tc(js("sensores", "ultrassom_profundidade_livre_cm"), "DOUBLE").alias("profundidade_livre_cm"),
        tc(js("sensores", "profundidade_nominal_cm"), "DOUBLE").alias("profundidade_nominal_cm"),
        tc(js("sensores", "nivel_agua_cm"), "DOUBLE").alias("nivel_agua_cm"),
        tc(js("sensores", "chuva_detectada"), "BOOLEAN").alias("chuva_detectada"),
        F.lower(F.trim(F.expr(js("visao", "classe")))).alias("classe_visao"),
        tc(js("visao", "confianca"), "DOUBLE").alias("confianca_visao"),
        F.expr(js("visao", "modelo")).alias("modelo_visao"),
        F.expr(js("visao", "foto_id")).alias("foto_id"),
        F.lower(F.trim(F.expr(js("ia_embarcada", "status")))).alias("status_ia"),
        tc(js("ia_embarcada", "obstrucao_pct"), "DOUBLE").alias("obstrucao_pct"),
        F.expr(js("operador", "nome")).alias("operador_nome"),          # dado pessoal: só sai da Prata como hash
        F.expr(js("operador", "matricula")).alias("operador_matricula"),
        F.expr(js("operador", "email")).alias("operador_email"),
        *LINHAGEM,
    ).withColumn("data_inspecao", F.to_date("inspecionado_em"))


processar(
    "inspecoes", "prata_inspecao", t_inspecao,
    regras=[
        ("INSP_01_ultrassom_valido", "profundidade livre presente e entre 0 e a profundidade nominal",
         F.col("profundidade_livre_cm").between(F.lit(0), F.col("profundidade_nominal_cm"))),
        ("INSP_02_visao_valida", "classe do YOLO conhecida e confiança entre 0 e 1",
         F.col("classe_visao").isin(*CLASSES_YOLO) & F.col("confianca_visao").between(0, 1)),
        ("INSP_03_gps_valido", "coordenada do robô dentro de São Paulo (não 0,0)",
         dentro_de_sp(F.col("latitude"), F.col("longitude"))),
    ],
    chave=["id_inspecao"],
)

# COMMAND ----------

# MAGIC %md ## Resultado das regras de qualidade nesta execução (print para o requisito 06)

# COMMAND ----------

display(spark.sql(f"""
  SELECT tabela, regra, descricao, sum(registros_no_lote) AS registros_avaliados, sum(barrados) AS barrados
  FROM {T('qualidade_regras')}
  WHERE executado_em >= '{inicio_execucao}'
  GROUP BY tabela, regra, descricao
  ORDER BY tabela, regra
"""))

# COMMAND ----------

# MAGIC %md ### Histórico das regras por execução (lote 1, lote 2, reexecução)

# COMMAND ----------

display(spark.sql(f"""
  SELECT date_format(executado_em, 'dd/MM HH:mm') AS execucao, tabela, regra,
         registros_no_lote AS registros_avaliados, barrados
  FROM {T('qualidade_regras')}
  ORDER BY executado_em, tabela, regra
"""))

# COMMAND ----------

# MAGIC %md ### Amostra da quarentena: o registro barrado inteiro, com as regras que violou

# COMMAND ----------

display(spark.sql(f"""
  SELECT date_format(executado_em, 'dd/MM HH:mm') AS execucao, tabela, regras_violadas,
         substr(registro, 1, 120) AS registro_inicio, element_at(split(arquivo_origem, '/'), -1) AS arquivo
  FROM {T('prata_quarentena')}
  WHERE tabela IN ('prata_inspecao', 'prata_solicitacao_sp156', 'prata_pluviometro')
  ORDER BY tabela, executado_em
  LIMIT 15
"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'prata_previsao_chuva' AS tabela, count(*) AS linhas FROM {T('prata_previsao_chuva')} UNION ALL
  SELECT 'prata_pluviometro', count(*) FROM {T('prata_pluviometro')} UNION ALL
  SELECT 'prata_area_risco', count(*) FROM {T('prata_area_risco')} UNION ALL
  SELECT 'prata_solicitacao_sp156', count(*) FROM {T('prata_solicitacao_sp156')} UNION ALL
  SELECT 'prata_inspecao', count(*) FROM {T('prata_inspecao')} UNION ALL
  SELECT 'prata_quarentena', count(*) FROM {T('prata_quarentena')}
"""))
