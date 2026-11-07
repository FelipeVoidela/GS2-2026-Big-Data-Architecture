-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Cinco consultas que o MVP responde
-- MAGIC
-- MAGIC | # | Pergunta | Quem usa |
-- MAGIC |---|---|---|
-- MAGIC | 1 | **IMPACTO:** quantos pontos críticos estão em área de risco com chuva prevista acima do limiar nas próximas 24 h, e quantas pessoas moram nessas áreas? | Defesa Civil, prefeitura, pitch |
-- MAGIC | 2 | Quais bueiros a equipe deve limpar primeiro hoje? | equipe de limpeza, bot de RPA |
-- MAGIC | 3 | Quais subprefeituras concentram bueiros entupidos em área de risco? | prefeitura, concessionária |
-- MAGIC | 4 | A situação está piorando ou melhorando de uma coleta para a outra? | gestão do contrato de limpeza |
-- MAGIC | 5 | Quanto tempo o cidadão espera quando pede limpeza de bueiro no SP156, e onde o robô confirma o problema? | cidadão, ouvidoria |
-- MAGIC
-- MAGIC O limiar de chuva é uma variável de sessão: troque o `DEFAULT` para simular outros cenários.

-- COMMAND ----------

USE workspace.gs;
DECLARE OR REPLACE VARIABLE limiar_mm DOUBLE DEFAULT 10.0;
DECLARE OR REPLACE VARIABLE data_ref DATE;
SET VAR data_ref = (SELECT max(data_inspecao) FROM ouro_fato_inspecao);
SELECT limiar_mm AS limiar_chuva_24h_mm, data_ref AS data_de_referencia;

-- COMMAND ----------

-- MAGIC %md ## Consulta 1 · IMPACTO — pontos críticos em área de risco com chuva prevista

-- COMMAND ----------

WITH hoje AS (
  SELECT f.*, b.situacao_risco, b.id_area_risco
  FROM ouro_fato_inspecao f
  JOIN ouro_dim_bueiro b ON b.id_bueiro = f.id_bueiro
  WHERE f.data_inspecao = data_ref
),
alvo AS (
  SELECT * FROM hoje
  WHERE status_ia = 'critico' AND situacao_risco IN ('dentro', 'vizinho') AND chuva_prevista_24h_mm >= limiar_mm
),
areas AS (
  SELECT DISTINCT a.id_area, a.qt_moradias, a.pessoas_estimadas
  FROM alvo JOIN ouro_dim_area_risco a ON a.id_area = alvo.id_area_risco
)
SELECT
  (SELECT count(*) FROM hoje)                                                         AS bueiros_inspecionados,
  (SELECT count(*) FROM hoje WHERE status_ia = 'critico')                             AS criticos,
  (SELECT count(*) FROM hoje WHERE status_ia = 'critico'
                              AND situacao_risco IN ('dentro', 'vizinho'))            AS criticos_em_area_de_risco,
  (SELECT count(*) FROM alvo)                                                         AS criticos_em_risco_com_chuva_24h,
  (SELECT count(DISTINCT subprefeitura) FROM alvo JOIN ouro_dim_bueiro USING (id_bueiro)) AS subprefeituras_afetadas,
  (SELECT count(*) FROM areas)                                                        AS areas_de_risco_afetadas,
  (SELECT coalesce(sum(qt_moradias), 0) FROM areas)                                   AS moradias_nas_areas,
  (SELECT coalesce(sum(pessoas_estimadas), 0) FROM areas)                             AS pessoas_estimadas,
  (SELECT round(max(chuva_prevista_24h_mm), 1) FROM alvo)                             AS maior_chuva_prevista_mm;

-- COMMAND ----------

-- MAGIC %md ## Consulta 2 · Rota de limpeza priorizada (top 15 do dia)

-- COMMAND ----------

SELECT row_number() OVER (ORDER BY f.score_prioridade DESC, f.obstrucao_pct DESC) AS ordem,
       f.id_bueiro, b.logradouro, b.distrito, b.subprefeitura,
       f.status_ia, f.obstrucao_pct, b.situacao_risco, b.grau_risco,
       f.chuva_prevista_24h_mm, f.pedidos_sp156_vizinhanca, f.score_prioridade, f.nivel_alerta
FROM ouro_fato_inspecao f
JOIN ouro_dim_bueiro b ON b.id_bueiro = f.id_bueiro
WHERE f.data_inspecao = data_ref AND f.status_ia IN ('critico', 'atencao')
ORDER BY ordem
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md ## Consulta 3 · Subprefeituras com mais bueiros críticos em área de risco

-- COMMAND ----------

SELECT b.subprefeitura,
       count(*)                                                                    AS inspecionados,
       count_if(f.status_ia = 'critico')                                           AS criticos,
       round(100 * count_if(f.status_ia = 'critico') / count(*), 1)                AS pct_criticos,
       count_if(f.status_ia = 'critico' AND b.situacao_risco <> 'fora')            AS criticos_em_risco,
       count_if(f.nivel_alerta = 'EVITE A AREA')                                   AS pontos_evite_a_area,
       round(avg(f.chuva_prevista_24h_mm), 1)                                      AS chuva_prevista_media_mm,
       sum(f.pedidos_sp156_vizinhanca)                                             AS pedidos_sp156_por_perto
FROM ouro_fato_inspecao f
JOIN ouro_dim_bueiro b ON b.id_bueiro = f.id_bueiro
WHERE f.data_inspecao = data_ref
GROUP BY b.subprefeitura
HAVING count(*) >= 3
ORDER BY criticos_em_risco DESC, pct_criticos DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md ## Consulta 4 · Evolução entre coletas (piorou ou melhorou?)

-- COMMAND ----------

SELECT d.data, d.dia_semana,
       count(*)                                                   AS inspecoes,
       count_if(f.status_ia = 'livre')                            AS livres,
       count_if(f.status_ia = 'atencao')                          AS atencao,
       count_if(f.status_ia = 'critico')                          AS criticos,
       round(avg(f.obstrucao_pct), 1)                             AS obstrucao_media_pct,
       round(avg(f.chuva_prevista_24h_mm), 1)                     AS chuva_prevista_media_mm,
       max(f.chuva_obs_72h_mm)                                    AS chuva_observada_72h_mm,
       count_if(f.nivel_alerta = 'EVITE A AREA')                  AS alertas_evite_a_area
FROM ouro_fato_inspecao f
JOIN ouro_dim_data d ON d.data = f.data_inspecao
GROUP BY d.data, d.dia_semana
ORDER BY d.data;

-- COMMAND ----------

-- MAGIC %md ## Consulta 5 · Espera do cidadão no SP156 x o que o robô encontra

-- COMMAND ----------

WITH sp156 AS (
  SELECT subprefeitura,
         count(*)                                             AS pedidos_limpeza_bueiro,
         percentile_approx(dias_atendimento, 0.5)             AS mediana_dias_atendimento,
         round(avg(dias_atendimento), 1)                      AS media_dias_atendimento,
         count_if(status = 'Aberto')                          AS ainda_abertos,
         round(100 * count_if(status = 'Aberto') / count(*), 1) AS pct_abertos
  FROM ouro_fato_solicitacao_sp156
  WHERE pedido_limpeza_bueiro
  GROUP BY subprefeitura
),
robo AS (
  SELECT b.subprefeitura, count_if(f.status_ia = 'critico') AS criticos_robo, count(*) AS inspecionados_robo
  FROM ouro_fato_inspecao f JOIN ouro_dim_bueiro b ON b.id_bueiro = f.id_bueiro
  WHERE f.data_inspecao = data_ref
  GROUP BY b.subprefeitura
)
SELECT s.*, r.inspecionados_robo, r.criticos_robo
FROM sp156 s LEFT JOIN robo r ON r.subprefeitura = s.subprefeitura
ORDER BY s.pedidos_limpeza_bueiro DESC
LIMIT 12;

-- COMMAND ----------

-- MAGIC %md ## Extra · O que o MVP lê (contrato) — amostra da view pública

-- COMMAND ----------

SELECT * FROM ouro_vw_mapa_cidadao ORDER BY score_prioridade DESC LIMIT 20;
