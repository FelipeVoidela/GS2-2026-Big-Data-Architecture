# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro — modelo dimensional, LGPD e contrato com o MVP
# MAGIC
# MAGIC **Requisitos 07 e 08.** Modelo estrela para a pergunta central do produto:
# MAGIC *"quais bocas de lobo entupidas estão em área de risco, com chuva forte prevista para as próximas 24 horas?"*
# MAGIC
# MAGIC ```
# MAGIC                    ouro_dim_data
# MAGIC                         |
# MAGIC ouro_dim_bueiro --- ouro_fato_inspecao --- (operador_hash)
# MAGIC        |
# MAGIC ouro_dim_area_risco          ouro_fato_solicitacao_sp156 --- ouro_dim_data
# MAGIC ```
# MAGIC
# MAGIC | Tabela | Grão | Fontes |
# MAGIC |---|---|---|
# MAGIC | `ouro_fato_inspecao` | 1 bueiro × 1 dia | robô + Open-Meteo + Cemaden + SP156 + GeoSampa |
# MAGIC | `ouro_fato_solicitacao_sp156` | 1 solicitação do cidadão | SP156 (com LGPD) |
# MAGIC | `ouro_dim_bueiro` | 1 boca de lobo | robô + GeoSampa (H3) |
# MAGIC | `ouro_dim_area_risco` | 1 setor de risco | GeoSampa |
# MAGIC | `ouro_dim_data` | 1 dia | calendário |
# MAGIC
# MAGIC **LGPD na Ouro:** nome, matrícula e e-mail do operador viram `operador_hash` (SHA-256 com sal);
# MAGIC o protocolo do SP156 vira hash, o número da casa vira **faixa** (ex.: 1800-1899), o CEP perde os 3 últimos dígitos
# MAGIC e a coordenada do pedido é arredondada para ~110 m. O sal não está no código (secret scope ou `config_lgpd`).
# MAGIC
# MAGIC **Contrato com o MVP:** o mapa público lê **somente** a view `ouro_vw_mapa_cidadao` (sem nenhum dado pessoal);
# MAGIC o bot de RPA e a equipe de limpeza leem `ouro_vw_rota_limpeza`.

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
CATALOGO, SCHEMA = "workspace", "gs"
spark.sql(f"USE {CATALOGO}.{SCHEMA}")

dbutils.widgets.text("limiar_chuva_mm", "10", "Chuva prevista em 24 h que dispara alerta (mm)")
dbutils.widgets.text("janela_sp156_dias", "120", "Janela de pedidos SP156 considerados (dias)")
LIMIAR = float(dbutils.widgets.get("limiar_chuva_mm"))
JANELA = int(dbutils.widgets.get("janela_sp156_dias"))

try:
    SAL = dbutils.secrets.get("gs", "sal_lgpd")
except Exception:  # noqa: BLE001 - sem secret scope: usa o sal guardado no Unity Catalog pelo notebook 00
    SAL = spark.table("config_lgpd").filter("chave = 'sal_lgpd'").first()["valor"]

PARAMS = {"limiar": LIMIAR, "janela": JANELA, "sal": SAL}  # parâmetros nomeados: o sal nunca entra no texto da query
print(f"Limiar de chuva: {LIMIAR} mm/24h | janela SP156: {JANELA} dias")


def criar_tabela(nome: str, comentario: str, consulta: str, params: dict | None = None) -> None:
    """Roda a consulta (com parâmetros nomeados) e grava a tabela Ouro por completo (overwrite)."""
    df = spark.sql(consulta, args=params) if params else spark.sql(consulta)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome)
    spark.sql(f"COMMENT ON TABLE {nome} IS '{comentario}'")

# COMMAND ----------

# MAGIC %md ## Dimensões

# COMMAND ----------

criar_tabela("ouro_dim_data", "Calendário", """
WITH limites AS (
  SELECT least((SELECT min(data_abertura) FROM prata_solicitacao_sp156), (SELECT min(data_inspecao) FROM prata_inspecao)) AS ini,
         date_add(greatest(current_date(), (SELECT max(data_inspecao) FROM prata_inspecao)), 7) AS fim
)
SELECT d AS data, year(d) AS ano, month(d) AS mes, day(d) AS dia,
       date_format(d, 'EEEE') AS dia_semana, dayofweek(d) IN (1, 7) AS fim_de_semana,
       month(d) IN (10, 11, 12, 1, 2, 3) AS periodo_chuvoso
FROM limites LATERAL VIEW explode(sequence(ini, fim)) s AS d
""")

criar_tabela("ouro_dim_area_risco", "Setores de risco hidrológico (GeoSampa)", """
SELECT id_area, nome_area, sigla_area, setor, grau_risco, grau_num, tipo_processo, qt_moradias,
       CAST(round(qt_moradias * 2.79) AS INT) AS pessoas_estimadas,   -- média de moradores por domicílio, Censo 2022
       subprefeitura, bacia, data_vistoria, geometria_geojson
FROM prata_area_risco
""")

criar_tabela("ouro_dim_bueiro", "Bocas de lobo inspecionadas, com situação de risco calculada por H3", """
WITH ultima AS (
  SELECT *, row_number() OVER (PARTITION BY id_bueiro ORDER BY inspecionado_em DESC) AS rn FROM prata_inspecao
),
b AS (
  SELECT id_bueiro, logradouro, distrito, subprefeitura,
         round(latitude, 5) AS latitude, round(longitude, 5) AS longitude, profundidade_nominal_cm,
         h3_longlatash3(longitude, latitude, 10) AS h3_10,
         h3_longlatash3(longitude, latitude, 9)  AS h3_9,
         concat_ws('_', CAST(CAST(round(latitude * 10) / 10 AS DECIMAL(5,1)) AS STRING),
                        CAST(CAST(round(longitude * 10) / 10 AS DECIMAL(5,1)) AS STRING)) AS celula_previsao_id
  FROM ultima WHERE rn = 1
),
risco AS (   -- área mais próxima (anel 0 antes do 1) e, no empate, a de maior grau
  SELECT b.id_bueiro, r.id_area, r.grau_num, r.anel,
         row_number() OVER (PARTITION BY b.id_bueiro ORDER BY r.anel, r.grau_num DESC) AS rn
  FROM b JOIN prata_area_risco_h3 r ON r.h3_10 = b.h3_10
)
SELECT b.*,
       CASE r.anel WHEN 0 THEN 'dentro' WHEN 1 THEN 'vizinho' ELSE 'fora' END AS situacao_risco,
       r.id_area AS id_area_risco, a.grau_risco, coalesce(r.grau_num, 0) AS grau_num, a.qt_moradias AS moradias_area
FROM b
LEFT JOIN risco r ON r.id_bueiro = b.id_bueiro AND r.rn = 1
LEFT JOIN ouro_dim_area_risco a ON a.id_area = r.id_area
""")
display(spark.sql("SELECT situacao_risco, count(*) AS bueiros FROM ouro_dim_bueiro GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

# MAGIC %md ## Fato principal: `ouro_fato_inspecao` (1 bueiro × 1 dia)
# MAGIC
# MAGIC `score_prioridade` (0 a 100) = status do robô (até 50) + área de risco (até 30) + chuva prevista (até 15) + pedidos do cidadão por perto (até 5).

# COMMAND ----------

criar_tabela("ouro_fato_inspecao", "Situação diária de cada boca de lobo (sem dados pessoais em claro)", """
WITH insp AS (
  SELECT *, row_number() OVER (PARTITION BY id_bueiro, data_inspecao ORDER BY inspecionado_em DESC) AS rn
  FROM prata_inspecao
),
chuva_prev AS (   -- previsão coletada no dia, para as 24 h a partir das 06h (hora em que o DAG roda)
  SELECT celula_id, data_coleta,
         round(sum(precipitacao_mm), 1) AS chuva_prevista_24h_mm, max(prob_chuva_pct) AS prob_chuva_max_pct
  FROM prata_previsao_chuva
  WHERE data_hora >= to_timestamp(data_coleta) + INTERVAL 6 HOURS
    AND data_hora <  to_timestamp(data_coleta) + INTERVAL 30 HOURS
  GROUP BY celula_id, data_coleta
),
chuva_obs AS (    -- solo encharcado: média dos pluviômetros ativos da capital nas últimas 72 h
  SELECT data_coleta, round(avg(acc_72h), 1) AS chuva_obs_72h_mm, count(*) AS estacoes_ativas
  FROM prata_pluviometro WHERE cod_ibge = 3550308
  GROUP BY data_coleta
),
pedidos AS (
  SELECT h3_longlatash3(longitude, latitude, 9) AS h3_9, data_abertura
  FROM prata_solicitacao_sp156
  WHERE servico ILIKE '%bueiro%' OR servico ILIKE '%boca de lobo%' OR assunto = 'Defesa Civil'
),
vizinhanca AS (SELECT id_bueiro, explode(h3_kring(h3_9, 1)) AS h3_9 FROM ouro_dim_bueiro),   -- raio ~0,5 km
demanda AS (
  SELECT i.id_inspecao, count(p.h3_9) AS pedidos_sp156_vizinhanca
  FROM insp i
  JOIN vizinhanca v ON v.id_bueiro = i.id_bueiro
  LEFT JOIN pedidos p ON p.h3_9 = v.h3_9
                     AND p.data_abertura BETWEEN date_sub(i.data_inspecao, :janela) AND i.data_inspecao
  WHERE i.rn = 1
  GROUP BY i.id_inspecao
),
base AS (
  SELECT i.id_inspecao, i.id_bueiro, i.data_inspecao, i.inspecionado_em, i.robo_id,
         sha2(concat(:sal, i.operador_matricula, lower(i.operador_email)), 256) AS operador_hash,
         i.status_ia, i.obstrucao_pct, i.profundidade_livre_cm, i.nivel_agua_cm,
         i.classe_visao, i.confianca_visao, i.modelo_visao,
         cp.chuva_prevista_24h_mm, cp.prob_chuva_max_pct, co.chuva_obs_72h_mm,
         coalesce(d.pedidos_sp156_vizinhanca, 0) AS pedidos_sp156_vizinhanca,
         b.situacao_risco, b.grau_num
  FROM insp i
  JOIN ouro_dim_bueiro b ON b.id_bueiro = i.id_bueiro
  LEFT JOIN chuva_prev cp ON cp.celula_id = b.celula_previsao_id AND cp.data_coleta = i.data_inspecao
  LEFT JOIN chuva_obs co ON co.data_coleta = i.data_inspecao
  LEFT JOIN demanda d ON d.id_inspecao = i.id_inspecao
  WHERE i.rn = 1
),
pontuado AS (
  SELECT *,
    CAST(round(
        CASE status_ia WHEN 'critico' THEN 50 WHEN 'atencao' THEN 25 ELSE 0 END
      + CASE situacao_risco WHEN 'dentro' THEN 1.0 WHEN 'vizinho' THEN 0.5 ELSE 0 END
        * CASE grau_num WHEN 4 THEN 30 WHEN 3 THEN 24 WHEN 2 THEN 16 WHEN 1 THEN 8 ELSE 0 END
      + least(coalesce(chuva_prevista_24h_mm, 0), 50) / 50 * 15
      + least(pedidos_sp156_vizinhanca, 5)
    ) AS INT) AS score_prioridade,
    situacao_risco IN ('dentro', 'vizinho') AS em_area_risco,
    coalesce(chuva_prevista_24h_mm, 0) >= :limiar AS chuva_acima_limiar
  FROM base
)
SELECT p.* EXCEPT (situacao_risco, grau_num),
  CASE
    WHEN status_ia = 'critico' AND em_area_risco AND chuva_acima_limiar THEN 'EVITE A AREA'
    WHEN status_ia = 'critico' OR (status_ia = 'atencao' AND em_area_risco AND chuva_acima_limiar) THEN 'ATENCAO'
    ELSE 'NORMAL'
  END AS nivel_alerta,
  CASE
    WHEN status_ia = 'critico' AND em_area_risco AND chuva_acima_limiar
      THEN 'Bueiro entupido em área que alaga e chuva forte prevista. Evite passar e não deixe o carro na rua.'
    WHEN status_ia = 'critico' THEN 'Bueiro entupido. Limpeza solicitada à equipe da região.'
    WHEN status_ia = 'atencao' THEN 'Bueiro parcialmente obstruído. Em observação.'
    ELSE 'Bueiro livre na última inspeção.'
  END AS recomendacao
FROM pontuado p
""", PARAMS)

# COMMAND ----------

# MAGIC %md ## Fato do cidadão: `ouro_fato_solicitacao_sp156` (LGPD aplicada)

# COMMAND ----------

criar_tabela("ouro_fato_solicitacao_sp156", "Pedidos do SP156 sobre drenagem. Sem protocolo, número de casa nem CEP completo (LGPD).", """
WITH ref AS (SELECT max(data_coleta) AS data_ref FROM prata_solicitacao_sp156)
SELECT
  sha2(concat(:sal, id_solicitacao), 256)                                   AS id_solicitacao_hash,
  data_abertura, data_finalizacao,
  CASE WHEN data_finalizacao IS NOT NULL THEN datediff(data_finalizacao, data_abertura) END AS dias_atendimento,
  CASE WHEN data_finalizacao IS NULL THEN datediff(ref.data_ref, data_abertura) END         AS dias_em_aberto,
  status, assunto, servico, canal, subprefeitura, distrito, logradouro,
  CASE WHEN try_cast(numero AS INT) IS NOT NULL THEN
       concat(CAST(floor(try_cast(numero AS INT) / 100) * 100 AS INT), '-',
              CAST(floor(try_cast(numero AS INT) / 100) * 100 + 99 AS INT)) END AS faixa_numeracao,
  CASE WHEN length(cep) = 8 THEN concat(substr(cep, 1, 5), '-***') END   AS cep_mascarado,
  round(latitude, 3)  AS lat_aprox,          -- ~110 m: mostra a rua, não a casa
  round(longitude, 3) AS lon_aprox,
  h3_longlatash3(longitude, latitude, 9) AS h3_9,
  (servico ILIKE '%bueiro%' OR servico ILIKE '%boca de lobo%') AS pedido_limpeza_bueiro
FROM prata_solicitacao_sp156 CROSS JOIN ref
""", PARAMS)

# COMMAND ----------

# MAGIC %md ## Contrato com o MVP

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW ouro_vw_mapa_cidadao
COMMENT 'CONTRATO DO MVP: única fonte do mapa público. Situação mais recente de cada boca de lobo, sem dados pessoais.' AS
WITH ult AS (
  SELECT f.*, row_number() OVER (PARTITION BY id_bueiro ORDER BY data_inspecao DESC) AS rn FROM ouro_fato_inspecao f
)
SELECT b.id_bueiro, b.latitude, b.longitude, b.logradouro, b.distrito, b.subprefeitura,
       u.status_ia AS status, u.obstrucao_pct, u.classe_visao, u.nivel_alerta, u.recomendacao,
       u.chuva_prevista_24h_mm, u.prob_chuva_max_pct, b.situacao_risco, b.grau_risco,
       u.score_prioridade, u.data_inspecao AS data_ultima_inspecao
FROM ult u JOIN ouro_dim_bueiro b ON b.id_bueiro = u.id_bueiro
WHERE u.rn = 1
""")

spark.sql("""
CREATE OR REPLACE VIEW ouro_vw_rota_limpeza
COMMENT 'Lista priorizada para a equipe de limpeza e para o bot de RPA (dia mais recente).' AS
SELECT dense_rank() OVER (PARTITION BY b.subprefeitura ORDER BY f.score_prioridade DESC) AS ordem_na_subprefeitura,
       b.subprefeitura, b.id_bueiro, b.logradouro, b.latitude, b.longitude,
       f.status_ia, f.obstrucao_pct, f.score_prioridade, f.nivel_alerta,
       f.chuva_prevista_24h_mm, b.situacao_risco, b.grau_risco, f.data_inspecao
FROM ouro_fato_inspecao f JOIN ouro_dim_bueiro b ON b.id_bueiro = f.id_bueiro
WHERE f.data_inspecao = (SELECT max(data_inspecao) FROM ouro_fato_inspecao)
  AND f.status_ia IN ('critico', 'atencao')
""")

display(spark.sql("SELECT nivel_alerta, count(*) AS bueiros FROM ouro_vw_mapa_cidadao GROUP BY 1 ORDER BY 2 DESC"))

# COMMAND ----------

# MAGIC %md ### Prova da LGPD na Ouro (print para o requisito 08)
# MAGIC À esquerda o que existe na Prata (acesso restrito), à direita o que a Ouro guarda.

# COMMAND ----------

display(spark.sql("""
SELECT 'operador' AS dado,
       concat(substr(first(p.operador_nome), 1, 3), '...') AS na_prata_restrita,
       concat(substr(first(f.operador_hash), 1, 16), '...') AS na_ouro
FROM prata_inspecao p JOIN ouro_fato_inspecao f ON f.id_inspecao = p.id_inspecao
UNION ALL
SELECT 'numero da casa (SP156)', '(número exato)', first(faixa_numeracao) FROM ouro_fato_solicitacao_sp156 WHERE faixa_numeracao IS NOT NULL
UNION ALL
SELECT 'CEP (SP156)', '(8 dígitos)', first(cep_mascarado) FROM ouro_fato_solicitacao_sp156 WHERE cep_mascarado IS NOT NULL
"""))

# COMMAND ----------

display(spark.sql("""
SELECT table_name, comment FROM information_schema.tables
WHERE table_schema = 'gs' AND table_name LIKE 'ouro_%' ORDER BY table_name
"""))

# COMMAND ----------

# MAGIC %md ### Exportação para o MVP
# MAGIC Gera `mapa_cidadao.json` no volume. A equipe baixa o arquivo e publica em `mvp/dados/` (ou o backend do MVP
# MAGIC consulta a view direto pela API de SQL do Databricks, com token guardado como segredo do deploy).

# COMMAND ----------

import json

linhas = [r.asDict() for r in spark.table("ouro_vw_mapa_cidadao").collect()]
for r in linhas:
    r["data_ultima_inspecao"] = str(r["data_ultima_inspecao"])
    r["obstrucao_pct"] = float(r["obstrucao_pct"]) if r["obstrucao_pct"] is not None else None
destino = f"/Volumes/{CATALOGO}/{SCHEMA}/chegada/_mvp/mapa_cidadao.json"
dbutils.fs.mkdirs(f"/Volumes/{CATALOGO}/{SCHEMA}/chegada/_mvp")
with open(destino, "w", encoding="utf-8") as fh:
    json.dump({"gerado_em": str(spark.sql("SELECT current_timestamp()").first()[0]),
               "fonte": f"{CATALOGO}.{SCHEMA}.ouro_vw_mapa_cidadao",
               "limiar_chuva_mm": LIMIAR, "pontos": linhas}, fh, ensure_ascii=False, default=str)
print(f"{len(linhas)} pontos exportados para {destino}")
