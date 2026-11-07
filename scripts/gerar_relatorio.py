"""
Gera relatorio/Relatorio_BigData_BocaDeLobo.docx na ordem pedida pelo professor (itens 01 a 09).

Prints do Airflow, do MVP e o diagrama já entram prontos. Trechos em AMARELO são o que a equipe ainda
precisa completar: prints do Databricks (que só existem depois de rodar os notebooks na conta da equipe)
e os resultados reais das consultas. As contagens da Bronze são calculadas a partir de lotes/.
Depois: Arquivo > Exportar > PDF.

Uso: python scripts/gerar_relatorio.py
"""
from __future__ import annotations

import tempfile
from pathlib import Path

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_COLOR_INDEX
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor
from PIL import Image

RAIZ = Path(__file__).resolve().parents[1]
PRINTS = RAIZ / "docs" / "prints"
SAIDA = RAIZ / "relatorio" / "Relatorio_BigData_BocaDeLobo.docx"
TMP = Path(tempfile.mkdtemp())

doc = Document()
for s in doc.sections:
    s.left_margin = s.right_margin = Cm(2)
    s.top_margin = s.bottom_margin = Cm(1.8)
estilo = doc.styles["Normal"]
estilo.font.name = "Calibri"
estilo.font.size = Pt(10.5)
n_fig = 0


def h(texto: str, nivel: int = 1):
    p = doc.add_heading(texto, level=nivel)
    for r in p.runs:
        r.font.color.rgb = RGBColor(0x0F, 0x2A, 0x4A)
    return p


def par(texto: str = "", negrito: bool = False, italico: bool = False, tam: float | None = None):
    p = doc.add_paragraph()
    r = p.add_run(texto)
    r.bold, r.italic = negrito, italico
    if tam:
        r.font.size = Pt(tam)
    p.paragraph_format.space_after = Pt(4)
    return p


def rico(*partes):
    """partes: (texto, negrito) ou texto."""
    p = doc.add_paragraph()
    for x in partes:
        t, b = (x, False) if isinstance(x, str) else x
        p.add_run(t).bold = b
    p.paragraph_format.space_after = Pt(4)
    return p


def pendente(texto: str):
    p = doc.add_paragraph()
    r = p.add_run(f"[COMPLETAR] {texto}")
    r.font.highlight_color = WD_COLOR_INDEX.YELLOW
    r.bold = True
    return p


def print_pendente(o_que: str):
    t = doc.add_table(rows=1, cols=1)
    t.style = "Table Grid"
    c = t.rows[0].cells[0]
    c.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = c.paragraphs[0].add_run(f"\n[INSERIR PRINT] {o_que}\n")
    r.font.highlight_color = WD_COLOR_INDEX.YELLOW
    r.italic = True
    legenda(f"(legenda) {o_que}")


def legenda(texto: str):
    global n_fig
    n_fig += 1
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(f"Figura {n_fig} - {texto}")
    r.italic = True
    r.font.size = Pt(9)
    r.font.color.rgb = RGBColor(0x47, 0x55, 0x69)


def imagem(arq: Path, texto: str, largura_cm: float = 17, corte_px: int | None = None):
    caminho = arq
    if corte_px:
        im = Image.open(arq)
        caminho = TMP / arq.name
        im.crop((0, 0, im.width, min(corte_px, im.height))).save(caminho)
    doc.add_picture(str(caminho), width=Cm(largura_cm))
    doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
    legenda(texto)


def print_ou_pendente(arquivo: str, texto: str, largura_cm: float = 17):
    """Usa o print de docs/prints/ se ele já existe; senão, deixa o espaço marcado para inserir."""
    caminho = PRINTS / arquivo
    if caminho.exists():
        imagem(caminho, texto, largura_cm)
    else:
        print_pendente(texto)


def tabela(cabecalho: list[str], linhas: list[list[str]], larguras: list[float] | None = None, tam: float = 9):
    t = doc.add_table(rows=1, cols=len(cabecalho))
    t.style = "Light Grid Accent 1"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    for i, c in enumerate(cabecalho):
        cel = t.rows[0].cells[i]
        cel.text = ""
        run = cel.paragraphs[0].add_run(c)
        run.bold = True
        run.font.size = Pt(tam)
    for ln in linhas:
        cells = t.add_row().cells
        for i, v in enumerate(ln):
            cells[i].text = ""
            run = cells[i].paragraphs[0].add_run(str(v))
            run.font.size = Pt(tam)
            if str(v).startswith("[COMPLETAR"):
                run.font.highlight_color = WD_COLOR_INDEX.YELLOW
    if larguras:
        for row in t.rows:
            for i, w in enumerate(larguras):
                row.cells[i].width = Cm(w)
    doc.add_paragraph()
    return t


def _linhas_arquivo(arq: Path) -> int:
    if arq.suffix == ".csv":
        import csv
        with open(arq, encoding="utf-8", newline="") as fh:
            return max(0, sum(1 for _ in csv.reader(fh)) - 1)
    texto = arq.read_text(encoding="utf-8").strip()
    if texto.startswith("["):
        import json
        return len(json.loads(texto))
    return sum(1 for ln in texto.splitlines() if ln.strip())


def linhas_bronze() -> list[list[str]]:
    """Contagens esperadas da Bronze a partir de lotes/lote_01 e lotes/lote_02 (ou [COMPLETAR] se o lote não existe)."""
    fmt = lambda n: f"{n:,}".replace(",", ".")  # noqa: E731
    linhas = []
    for fonte in ["previsao", "pluviometros", "areas_risco", "sp156", "inspecoes"]:
        por_lote = []
        for lote in ("lote_01", "lote_02"):
            arqs = sorted((RAIZ / "lotes" / lote / fonte).glob(f"{fonte}_*.*"))
            por_lote.append(sum(_linhas_arquivo(a) for a in arqs) if arqs else None)
        l1, l2 = por_lote
        if l1 is None or l2 is None:
            linhas.append([f"bronze_{fonte}", fmt(l1) if l1 is not None else "[COMPLETAR]", "[COMPLETAR]", "[COMPLETAR]", "0"])
        else:
            linhas.append([f"bronze_{fonte}", fmt(l1), fmt(l1 + l2), fmt(l1 + l2), "0"])
    return linhas


def codigo(texto: str):
    p = doc.add_paragraph()
    r = p.add_run(texto)
    r.font.name = "Consolas"
    r._element.rPr.rFonts.set(qn("w:eastAsia"), "Consolas")
    r.font.size = Pt(8)
    sombra = OxmlElement("w:shd")
    sombra.set(qn("w:val"), "clear")
    sombra.set(qn("w:fill"), "F1F5F9")
    p._p.get_or_add_pPr().append(sombra)


# ------------------------------------------------------------------ Capa
p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = p.add_run("\n\nFIAP · Global Solution 2026.2 · CitizenTech\nEngenharia de Software · 4º ano")
r.font.size = Pt(12)
p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = p.add_run("\nBoca de Lobo\n")
r.bold = True
r.font.size = Pt(30)
r.font.color.rgb = RGBColor(0x0F, 0x2A, 0x4A)
p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = p.add_run("Componente Dados: pipeline Airflow + Lakehouse Bronze/Prata/Ouro no Databricks\n\n"
              "Big Data Architecture & Data Integration\n\n"
              "Segurança e emergências (enchentes) · ODS 11, 13 e 3")
r.font.size = Pt(13)
p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
r = p.add_run("\n\nRepositório: https://github.com/FelipeVoidela/GS2-2026-Big-Data-Architecture")
r.bold = True
doc.add_page_break()

# ------------------------------------------------------------------ 01
h("01 · Integrantes")
tabela(["Nome completo", "RM"], [
    ["Açussena Macedo Mautone", "552568"],
    ["Felipe Heilmann Marques", "551026"],
    ["Felipe Voidela Toledo", "98595"],
    ["Carlos Eduardo Caramante Ribeiro", "552159"],
    ["Ian Cancian Nachtergaele", "98387"],
], [12, 4], tam=10.5)

# ------------------------------------------------------------------ 02
h("02 · O produto em uma frase e as perguntas que o MVP responde")
rico(("Boca de Lobo: ", True),
     "antes de cada temporal, um robô inspeciona as bocas de lobo do bairro; quando a previsão indica chuva forte, "
     "o sistema cruza os bueiros entupidos com as áreas de risco e avisa a equipe de limpeza (rota priorizada) e o "
     "morador (onde não passar).")
par("Cidadão: morador de região que alaga todo verão, principalmente em áreas baixas e periféricas de São Paulo.")
par("Perguntas que o MVP responde com os dados deste pipeline:", negrito=True)
tabela(["#", "Pergunta", "Quem pergunta", "Tabela/consulta"], [
    ["1", "Quantos bueiros entupidos estão em área de risco com chuva forte prevista para as próximas 24 h, e quantas pessoas moram ali? (IMPACTO)", "Defesa Civil, prefeitura", "Consulta 1"],
    ["2", "Quais bueiros a equipe deve limpar primeiro hoje?", "equipe de limpeza, bot RPA", "Consulta 2 / ouro_vw_rota_limpeza"],
    ["3", "Quais subprefeituras concentram bueiros críticos em área de risco?", "prefeitura, concessionária", "Consulta 3"],
    ["4", "A situação piorou ou melhorou de uma coleta para a outra?", "gestão do contrato", "Consulta 4"],
    ["5", "Quanto o cidadão espera quando pede limpeza no SP156, e o robô confirma o problema?", "cidadão, ouvidoria", "Consulta 5"],
    ["6", "Tem bueiro entupido perto de mim? Devo evitar a minha rua hoje?", "morador", "ouro_vw_mapa_cidadao (tela do MVP)"],
], [0.8, 9, 3.5, 3.7])

# ------------------------------------------------------------------ 03
h("03 · Diagrama do fluxo de dados, da fonte ao MVP")
imagem(RAIZ / "docs" / "arquitetura.png", "Fluxo de dados: fontes → Airflow → chegada/ → Volume → Bronze → Prata → Ouro → MVP, RPA e BI")
par("O Databricks Free Edition não se conecta ao Airflow: os arquivos de chegada/ sobem ao Volume pela interface, em lotes "
    "de dias de coleta diferentes (seta tracejada).", italico=True, tam=9.5)

# ------------------------------------------------------------------ 04
h("04 · Tabela de fontes")
tabela(["Fonte", "Origem", "Formato", "Frequência", "Volume por coleta"], [
    ["Open-Meteo (API externa)", "api.open-meteo.com/v1/forecast", "JSON (array)", "diária 06h", "56 pontos × 48 h"],
    ["Cemaden (dado aberto externo)", "resources.cemaden.gov.br · getJson2.php?uf=SP", "JSON (array)", "diária 06h", "724 estações (79 na capital)"],
    ["GeoSampa (WFS da prefeitura)", "camada geoportal:risco_hidrologico", "GeoJSON → JSON Lines", "diária 06h", "793 setores de risco"],
    ["SP156 (dado aberto)", "dados.prefeitura.sp.gov.br (CKAN, trimestre mais recente)", "CSV", "diária 06h (cache por trimestre)", "10.758 pedidos de drenagem"],
    ["Robô Maker Lab + YOLO (simulado)", "dags/boca_de_lobo/simulador_robo.py", "JSON Lines", "diária 06h", "≈200 inspeções"],
], [3.6, 5.2, 2.6, 2.6, 3.2], tam=8.5)
par("Dado simulado, com justificativa: o robô e o YOLO ainda estão em construção nas disciplinas de Maker Lab e Visão. O "
    "simulador produz exatamente o JSON que o robô vai enviar, sobre 300 pontos reais (endereços de pedidos de limpeza de "
    "boca de lobo no SP156, 60% perto de área de risco), com acúmulo de lixo realista e ~6% de defeitos propositais para testar "
    "as regras da Prata. É determinístico por data (coleta idempotente) e usa operadores fictícios.", tam=9.5)

# ------------------------------------------------------------------ 05
h("05 · Requisitos 01 a 09")

h("Requisito 01 · Fontes de dados do problema", 2)
par("Cinco fontes, quatro delas externas e reais (Open-Meteo, Cemaden, GeoSampa e SP156) e uma interna (robô). Todas falam do "
    "mesmo problema: chuva prevista e observada, onde alaga, onde o cidadão já reclamou e qual bueiro está entupido. A "
    "conferência do Airflow registra quantos registros chegaram de cada uma:")
imagem(PRINTS / "airflow_log_conferir_chegada.png", "Log da tarefa conferir_chegada com os registros recebidos de cada fonte em 07/11/2026", corte_px=380)

h("Requisito 02 · DAG agendado gravando em chegada/", 2)
par("DAG gs_boca_de_lobo_coleta, agendado para 06h todo dia (cron 0 6 * * *, fuso de São Paulo), uma hora antes do bot de RPA. "
    "Cada tarefa grava um arquivo por coleta em chegada/<fonte>/<fonte>_AAAA-MM-DD.<ext>, de forma atômica (.tmp + rename) e "
    "idempotente (rodar de novo sobrescreve o mesmo arquivo).")
imagem(PRINTS / "airflow_codigo.png", "Código do DAG no Airflow: agendamento, tarefas e dependências")
codigo("chegada/\n  previsao/previsao_2026-11-07.json      previsao/previsao_2026-11-08.json\n"
       "  pluviometros/pluviometros_2026-11-07.json  ...\n  areas_risco/areas_risco_2026-11-07.json  ...\n"
       "  sp156/sp156_2026-11-07.csv              ...\n  inspecoes/inspecoes_2026-11-07.json      ...")

h("Requisito 03 · Paralelismo, verificação e chaves fora do código", 2)
par("inicio >> [5 coletas em paralelo] >> conferir_chegada >> notificar. A conferência falha se algum arquivo do dia não existe, "
    "está vazio ou ilegível. Cada coleta tem 2 novas tentativas e on_failure_callback, que avisa por webhook. A URL do webhook e a "
    "chave do Open-Meteo (plano comercial) ficam em Variables do Airflow (gs_alerta_webhook_url, gs_open_meteo_apikey), nunca no "
    "código. A tarefa notificar falha de propósito se a conferência não passou, para a execução ficar vermelha: num sistema de "
    "emergência, a falha silenciosa é o pior cenário.")
imagem(PRINTS / "airflow_graph.png", "Visão Graph: 5 coletas em paralelo, conferência e notificação, todas com sucesso")
imagem(PRINTS / "airflow_log_notificar.png", "Log da tarefa notificar (sem webhook configurado, a mensagem vai para o log)", corte_px=380)
print_pendente("Admin > Variables com gs_alerta_webhook_url e gs_open_meteo_apikey cadastradas (valores mascarados)")

h("Requisito 04 · Dois lotes enviados ao Volume", 2)
par("scripts/montar_lotes.py separa os arquivos por dia de coleta: lote_01 = execução agendada de 07/11 (06h), lote_02 = "
    "execução agendada de 08/11 (06h). Cada lote foi enviado pela interface do Databricks para "
    "/Volumes/workspace/gs/chegada/<fonte>/.")
print_ou_pendente("databricks_volume.jpg", "Volume workspace.gs.chegada, pasta inspecoes: um arquivo por dia de coleta (lote 1 e lote 2)")
print_ou_pendente("databricks_upload_lote2.jpg", "Envio do 2º lote pela interface (Upload to this volume)")

h("Requisito 05 · Bronze incremental e sem duplicação", 2)
par("Auto Loader (cloudFiles) com checkpoint por fonte e trigger(availableNow=True). Tudo é lido como texto (inferColumnTypes = "
    "false) e cada linha ganha arquivo_origem, data_coleta e carregado_em. No 2º lote só os arquivos novos entram; rodar de novo "
    "sem arquivo novo insere 0 linhas. As contagens estão no item 07.")
print_ou_pendente("databricks_bronze_lote1.jpg", "Notebook 01 depois do lote 1: o Auto Loader carregou 1 arquivo por fonte (linhas_antes = 0)")

h("Requisito 06 · Prata: tratamento, MERGE e regras de qualidade", 2)
par("Cada fonte é lida da Bronze em streaming (só o que é novo) e cada micro-lote passa por: tipagem com try_cast/try_to_timestamp "
    "(o serverless roda em modo ANSI), 3 regras de qualidade com contagem por regra, quarentena dos registros barrados, "
    "deduplicação pela chave e MERGE INTO pela chave.")
tabela(["Tabela", "Regra", "O que barra"], [
    ["prata_inspecao", "INSP_01_ultrassom_valido", "profundidade livre nula, negativa ou maior que a nominal"],
    ["prata_inspecao", "INSP_02_visao_valida", "classe do YOLO desconhecida ou confiança fora de 0–1"],
    ["prata_inspecao", "INSP_03_gps_valido", "coordenada fora de SP (GPS 0,0)"],
    ["prata_pluviometro", "PLUV_01/02/03", "leitura '-', estação parada há mais de 2 dias, acumulados incoerentes"],
    ["prata_solicitacao_sp156", "SP156_01/02/03", "sem coordenada, data de abertura inválida, finalização antes da abertura"],
    ["prata_previsao_chuva", "PREV_01/02/03", "precipitação nula ou absurda, probabilidade fora de 0–100, ponto fora de SP"],
    ["prata_area_risco", "AREA_01/02/03", "grau fora de R1–R4, polígono ausente, moradias negativas"],
    ["todas", "DEDUP", "registros repetidos pela chave no lote (ex.: reenvio do robô)"],
], [4, 4.3, 8.7], tam=8.5)
print_ou_pendente("databricks_prata_regras_a.jpg", "Notebook 02: total barrado por regra de qualidade em cada execução (parte 1)")
print_ou_pendente("databricks_prata_regras_b.png", "Notebook 02: total barrado por regra de qualidade em cada execução (parte 2)")
print_ou_pendente("databricks_prata_quarentena.jpg", "prata_quarentena: o registro barrado inteiro e as regras que ele violou")

h("Requisito 07 · Ouro dimensional e 5 consultas", 2)
par("Modelo estrela: ouro_fato_inspecao (1 bueiro × 1 dia) com ouro_dim_bueiro, ouro_dim_area_risco e ouro_dim_data; e um segundo fato, "
    "ouro_fato_solicitacao_sp156. O cruzamento bueiro × área de risco usa índice H3 (resolução 10, ~66 m, com vizinhança de 1 anel): "
    "vira um JOIN por número inteiro, sem cálculo geométrico pesado. O score_prioridade (0–100) soma status do robô (até 50), grau da "
    "área de risco (até 30), chuva prevista (até 15) e pedidos do SP156 por perto (até 5). As 5 consultas estão no item 08.")
print_ou_pendente("databricks_ouro_tabelas.jpg", "Tabelas e views da Ouro no schema workspace.gs, com a descrição de cada uma")

h("Requisito 08 · LGPD na Ouro e uso pelo MVP", 2)
tabela(["Dado pessoal", "Na Prata (restrita)", "Na Ouro"], [
    ["Operador do robô (nome, matrícula, e-mail)", "em claro", "operador_hash = SHA-256(sal + matrícula + e-mail)"],
    ["Protocolo do pedido SP156", "hash do registro", "SHA-256(sal + id)"],
    ["Número da casa", "exato", "faixa (ex.: 1800-1899)"],
    ["CEP", "8 dígitos", "05409-***"],
    ["Coordenada do pedido", "7 casas", "3 casas (~110 m)"],
], [6, 3.5, 7.5], tam=9)
par("O sal é gerado dentro do Databricks (secret scope ou tabela config_lgpd) e passado às consultas como parâmetro nomeado, "
    "então não aparece no código nem no texto da query. Contrato com o produto: o mapa público lê SOMENTE a view "
    "ouro_vw_mapa_cidadao (id do bueiro, coordenada, rua, status, nível de alerta, chuva prevista, situação de risco), que não tem "
    "dado pessoal. O bot de RPA lê ouro_vw_rota_limpeza.")
print_ou_pendente("databricks_ouro_lgpd.jpg", "Notebook 03, prova da LGPD: o que a Prata guarda (restrita) e o que a Ouro expõe (hash, faixa e máscara)")
imagem(PRINTS / "mvp_evite_a_area.png", "Tela do MVP lendo ouro_vw_mapa_cidadao: filtro 'Evite a área' e detalhe de um bueiro crítico em área R3")
imagem(PRINTS / "mvp_celular.png", "Tela do MVP no celular: mapa, resumo por nível de alerta e chuva prevista", largura_cm=7)
pendente("Os prints do MVP acima usam a prévia local (scripts/preview_local.py). Depois de rodar o notebook 03, troque o "
         "mvp/dados/mapa_cidadao.json pelo export do Databricks e refaça os prints.")

h("Requisito 09 · Repositório e README", 2)
par("README com integrantes, ficha técnica, arquitetura, tabela de fontes, passo a passo para reproduzir (Airflow, lotes, "
    "Databricks, MVP), mapa requisito → arquivo e LGPD. Estrutura: dags/, databricks/, amostras/, mvp/, scripts/, docs/. "
    "Sem chaves, senhas nem dados pessoais reais (.env, chegada/ e lotes/ no .gitignore; amostra do SP156 com número e CEP mascarados).")
print_ou_pendente("github_readme.jpg", "Repositório público no GitHub com o README renderizado")

# ------------------------------------------------------------------ 06
doc.add_page_break()
h("06 · Prints do Airflow: Graph e Grid com execuções concluídas")
imagem(PRINTS / "airflow_grid.png", "Visão Grid: execuções agendadas do DAG, todas as tarefas em verde")
imagem(PRINTS / "airflow_graph.png", "Visão Graph de uma execução agendada (06h): coletas em paralelo, conferência e notificação")
imagem(PRINTS / "airflow_execucoes.png", "Lista de execuções do DAG gs_boca_de_lobo_coleta")

# ------------------------------------------------------------------ 07
h("07 · Contagens da Bronze antes e depois do 2º lote e da 2ª execução")
par("Valores esperados a partir dos arquivos de lotes/ (conferir com o print da tabela controle_cargas):")
tabela(["Tabela", "Após lote 1", "Após lote 2", "2ª execução sem arquivo novo", "Linhas inseridas na 2ª execução"],
       linhas_bronze(), [4, 2.6, 2.6, 3.6, 4.2], tam=9)
par("Leitura: no 2º lote cada tabela recebeu só o arquivo novo (o número de arquivos distintos passa de 1 para 2 e as linhas "
    "somam as dos dois dias). Na execução seguinte, sem arquivo novo, o checkpoint do Auto Loader "
    "não relê nada e a contagem fica igual: não há duplicação. A repetição de conteúdo entre dias (ex.: o mesmo trimestre do "
    "SP156) é resolvida na Prata pelo MERGE na chave.", tam=9.5)
print_ou_pendente("databricks_bronze_historico.jpg", "Notebook 01, histórico das cargas: lote 1, lote 2 e reexecução sem arquivo novo (0 linhas inseridas)")

# ------------------------------------------------------------------ 08
h("08 · As 5 consultas SQL com resultado e interpretação")
par("Arquivo databricks/04_consultas_mvp.sql. Limiar de chuva: 10 mm em 24 h (variável limiar_mm).", italico=True, tam=9.5)
pendente("As interpretações abaixo foram escritas com a prévia local. Depois de rodar o notebook 04 com os dois lotes, "
         "troque os números pelos do resultado do Databricks (a data de referência passa a ser a do lote 2).")

consultas = [
    ("Consulta 1 · IMPACTO: críticos em área de risco com chuva prevista",
     "WITH hoje AS (... ouro_fato_inspecao JOIN ouro_dim_bueiro WHERE data_inspecao = data_ref),\n"
     "alvo AS (SELECT * FROM hoje WHERE status_ia = 'critico'\n"
     "         AND situacao_risco IN ('dentro','vizinho') AND chuva_prevista_24h_mm >= limiar_mm),\n"
     "areas AS (SELECT DISTINCT a.id_area, a.qt_moradias, a.pessoas_estimadas FROM alvo JOIN ouro_dim_area_risco a ...)\n"
     "SELECT bueiros_inspecionados, criticos, criticos_em_area_de_risco, criticos_em_risco_com_chuva_24h,\n"
     "       areas_de_risco_afetadas, moradias_nas_areas, pessoas_estimadas ...",
     "Em 07/11/2026 o robô inspecionou 192 bocas de lobo: 74 estavam críticas, 37 delas dentro ou ao lado de área de risco "
     "hidrológico e 8 nessas condições com chuva prevista de 10 mm ou mais nas próximas 24 h. Essas 8 ficam em 7 setores de "
     "risco com 1.421 moradias, cerca de 3.965 pessoas (2,79 moradores por domicílio, Censo 2022). É a lista que vira alerta "
     "'Evite a área' no mapa e a prioridade zero da equipe de limpeza. Com limiar de 5 mm, seriam 37 pontos e ~25 mil pessoas."),
    ("Consulta 2 · Rota de limpeza priorizada (top 15)",
     "SELECT row_number() OVER (ORDER BY score_prioridade DESC, obstrucao_pct DESC) AS ordem, id_bueiro, logradouro,\n"
     "       subprefeitura, status_ia, obstrucao_pct, situacao_risco, grau_risco, chuva_prevista_24h_mm, score_prioridade\n"
     "FROM ouro_fato_inspecao f JOIN ouro_dim_bueiro b ... WHERE data_inspecao = data_ref AND status_ia IN ('critico','atencao')",
     "O topo da rota é a Rua Simão Rocha (Jabaquara), crítica dentro de área R4 (score 86), seguida de três pontos em áreas R3 do "
     "Jaçanã-Tremembé e um em São Miguel (score 82). O score põe na frente quem combina bueiro entupido + área que alaga + chuva, "
     "e não só quem está mais sujo: é a rota que a concessionária paga para receber."),
    ("Consulta 3 · Subprefeituras com mais críticos em área de risco",
     "SELECT subprefeitura, count(*) inspecionados, count_if(status_ia='critico') criticos, pct_criticos,\n"
     "       count_if(status_ia='critico' AND situacao_risco<>'fora') criticos_em_risco, ...\n"
     "GROUP BY subprefeitura HAVING count(*) >= 3 ORDER BY criticos_em_risco DESC",
     "M'Boi Mirim e São Miguel lideram com 6 bueiros críticos em área de risco cada, seguidas de Campo Limpo (5) e "
     "Jaçanã-Tremembé (3): todas periféricas, o mesmo território do cidadão do tema. É onde a primeira equipe deve ir."),
    ("Consulta 4 · Evolução entre coletas",
     "SELECT d.data, count(*) inspecoes, count_if(status_ia='livre') livres, count_if(status_ia='atencao') atencao,\n"
     "       count_if(status_ia='critico') criticos, avg(obstrucao_pct), avg(chuva_prevista_24h_mm), ...\n"
     "FROM ouro_fato_inspecao f JOIN ouro_dim_data d ON d.data = f.data_inspecao GROUP BY d.data",
     "De 06/11 para 07/11 os críticos subiram de 68 para 74 (de 188 e 192 inspeções válidas) e a obstrução média passou de "
     "56,1% para 56,8%: sem limpeza, o lixo acumula. Com o pipeline rodando todo dia, a mesma consulta mostra o efeito de "
     "cada mutirão de limpeza e serve de indicador do contrato com a concessionária."),
    ("Consulta 5 · Espera do cidadão no SP156 x o que o robô encontra",
     "WITH sp156 AS (SELECT subprefeitura, count(*) pedidos_limpeza_bueiro, percentile_approx(dias_atendimento,0.5),\n"
     "       count_if(status='Aberto') ainda_abertos ... FROM ouro_fato_solicitacao_sp156 WHERE pedido_limpeza_bueiro ...),\n"
     "robo AS (... criticos_robo por subprefeitura) SELECT ... FROM sp156 LEFT JOIN robo ...",
     "No 3º trimestre de 2026 foram 5.757 pedidos de limpeza de bueiro no SP156; 9,4% seguiam abertos na coleta. A mediana de "
     "atendimento é baixa (1 dia), mas varia muito: na Sé e em Pinheiros cerca de 1 em cada 3 pedidos ainda estava aberto, "
     "enquanto em São Miguel quase todos foram fechados. Cruzando com o robô dá para ver onde o pedido foi fechado e o bueiro "
     "continua crítico, um argumento para a prefeitura contratar a inspeção."),
]
for titulo, sql, interp in consultas:
    h(titulo, 2)
    codigo(sql)
    n = titulo.split(" ·")[0].split()[-1]
    print_ou_pendente(f"databricks_consulta{n}.jpg", f"Resultado da {titulo.split(' ·')[0]} no notebook 04")
    rico(("Interpretação: ", True), interp)

# ------------------------------------------------------------------ 09
h("09 · Conclusão técnica e link do repositório")
par("O componente Dados coleta todos os dias, às 06h, cinco fontes que juntas respondem à pergunta do tema: onde há bueiro "
    "entupido em área que alaga com chuva forte a caminho. O Airflow roda as coletas em paralelo, confere a chegada e avisa quando "
    "uma fonte cai. No Databricks, a Bronze é incremental e não duplica (Auto Loader com checkpoint), a Prata aplica 3 regras de "
    "qualidade por fonte com contagem e quarentena e faz MERGE pela chave, e a Ouro organiza tudo em estrela, com o cruzamento "
    "espacial resolvido por H3 e os dados pessoais protegidos por hash, faixa e máscara.")
par("Decisões que vale destacar: (1) GeoSampa gravado com a geometria como texto GeoJSON, para o Auto Loader não depender de "
    "inferir arrays de coordenadas aninhados; (2) try_cast em toda a Prata, porque o serverless roda em ANSI e o Cemaden manda '-' "
    "quando a estação não mede; (3) chave por hash no SP156, que não publica protocolo; (4) o MVP lê só uma view sem dado pessoal, "
    "o que torna o contrato com o produto explícito.")
par("Limitações e próximos passos: os pluviômetros do endpoint público do Cemaden não trazem coordenadas (a chuva observada entra "
    "como média da capital; com o token da API PED dá para ligar cada estação ao bairro); o SP156 aberto é trimestral; e o robô é "
    "simulado até o Maker Lab entregar o carrinho, quando só a função coletar_inspecoes muda.")
rico(("Repositório público: ", True), "https://github.com/FelipeVoidela/GS2-2026-Big-Data-Architecture")

SAIDA.parent.mkdir(parents=True, exist_ok=True)
doc.save(SAIDA)
print(SAIDA)
