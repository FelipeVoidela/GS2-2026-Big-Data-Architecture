"""Desenha docs/arquitetura.png (fluxo de dados da fonte ao MVP). Uso: python scripts/desenhar_arquitetura.py"""
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

RAIZ = Path(__file__).resolve().parents[1]
fig, ax = plt.subplots(figsize=(17, 8.2), dpi=150)
ax.set_xlim(0, 170)
ax.set_ylim(0, 82)
ax.axis("off")

AZUL, VERDE, BRONZE, PRATA, OURO, CINZA = "#dbe7f7", "#e2f2e6", "#f1dcc4", "#e6e8ec", "#f8e7a8", "#f4f4f4"


def caixa(x, y, w, h, titulo, texto="", cor=CINZA, borda="#334155", tam=10):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.5",
                                fc=cor, ec=borda, lw=1.3))
    ax.text(x + w / 2, y + h - 2.2, titulo, ha="center", va="top", fontsize=tam, fontweight="bold", color="#0f172a")
    if texto:
        ax.text(x + w / 2, y + h - 6.0, texto, ha="center", va="top", fontsize=tam - 2.2, color="#334155", linespacing=1.35)


def seta(x1, y1, x2, y2, rotulo="", cor="#334155", estilo="-|>", tracejada=False, pos=None, ha="center"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle=estilo, mutation_scale=14, lw=1.4, color=cor,
                                 linestyle="--" if tracejada else "-"))
    if rotulo:
        tx, ty = pos if pos else ((x1 + x2) / 2, (y1 + y2) / 2 + 2.5)
        ax.text(tx, ty, rotulo, ha=ha, va="bottom", fontsize=7.6, color=cor, style="italic")


# Título
ax.text(2, 79.5, "Boca de Lobo · fluxo de dados da fonte ao MVP", fontsize=15, fontweight="bold", color="#0f172a")
ax.text(2, 76.2, "Coleta diária às 06h no Airflow  →  upload em lotes ao Volume  →  Bronze / Prata / Ouro no Databricks  →  mapa do cidadão e rota de limpeza",
        fontsize=9.5, color="#475569")

# Fontes
fontes = [
    ("Open-Meteo (API)", "previsão horária de chuva\ngrade de 56 pontos, 48 h"),
    ("Cemaden (dados abertos)", "pluviômetros: chuva\nacumulada 1 h a 96 h"),
    ("GeoSampa (WFS)", "793 setores de risco\nhidrológico R1–R4"),
    ("SP156 (dados abertos)", "pedidos de limpeza de\nbueiro e alagamento"),
    ("Robô Maker Lab + YOLO", "inspeções: ultrassom,\nclasse visual, status"),
]
ax.text(13, 71.5, "FONTES", ha="center", fontsize=9, fontweight="bold", color="#1e3a8a")
for i, (t, d) in enumerate(fontes):
    y = 59 - i * 13
    caixa(2, y, 22, 11, t, d, cor=AZUL if i < 4 else VERDE, tam=8.8)
    seta(24.6, y + 5.5, 32, 38.5)

# Airflow
caixa(32, 22, 27, 34, "Airflow 3 (Docker)",
      "DAG gs_boca_de_lobo_coleta\n@ 06h todo dia\n\ninicio\n↓\n5 coletas em PARALELO\n↓\nconferir_chegada\n(arquivo existe, legível, >0)\n↓\nnotificar (webhook)\n\nchaves em Variables\nalerta on_failure_callback",
      cor="#e0f2fe", tam=9.5)
seta(59.5, 39, 66, 39, "1 arquivo\npor fonte/dia", pos=(62.7, 41))
caixa(66, 30, 19, 18, "chegada/", "<fonte>/\n<fonte>_AAAA-MM-DD\n.json | .csv\n\nlotes/lote_01, 02", cor=CINZA, tam=9.5)
seta(85.5, 39, 92.5, 39, "upload\n(lotes)", tracejada=True, pos=(89, 41))

# Databricks
ax.add_patch(FancyBboxPatch((90, 8), 52, 62, boxstyle="round,pad=0.5,rounding_size=2", fc="#fff7ed", ec="#c2410c", lw=1.6, ls="--"))
ax.text(116, 68.2, "Databricks Free Edition · workspace.gs", ha="center", fontsize=10, fontweight="bold", color="#9a3412")
caixa(93, 55, 46, 9, "Volume chegada/", "/Volumes/workspace/gs/chegada/<fonte>/", cor="#ffffff", tam=9)
seta(116, 54.5, 116, 49.5)
caixa(93, 38, 46, 11, "BRONZE · Auto Loader", "5 tabelas bronze_*  · só arquivos novos (checkpoint)\n+ arquivo_origem, data_coleta, carregado_em", cor=BRONZE, tam=9)
seta(116, 37.5, 116, 34.5)
caixa(93, 23, 46, 11, "PRATA · tipos + regras + MERGE", "try_cast · 3 regras/fonte com contagem · quarentena\ndedup · MERGE pela chave · H3 das áreas de risco", cor=PRATA, tam=9)
seta(116, 22.5, 116, 20.5)
caixa(93, 9.5, 46, 10.5, "OURO · estrela + LGPD", "fato_inspecao · dim_bueiro · dim_area_risco · dim_data\nfato_solicitacao_sp156 · hash/máscara", cor=OURO, tam=8.6)

# Consumo
seta(139.5, 15, 146.5, 56, "lê ouro_vw_mapa_cidadao", pos=(157.5, 64.9))
seta(139.5, 15, 146.5, 34, "lê ouro_vw_rota_limpeza", pos=(157.5, 42.9))
seta(139.5, 15, 146.5, 13, "5 consultas SQL", pos=(157.5, 20.9))
caixa(147, 50, 21, 14, "MVP · mapa público", "Evite a área / Atenção /\nNormal, chuva 24 h\n(sem dado pessoal)", cor=VERDE, tam=9)
caixa(147, 28, 21, 14, "Bot RPA (07h)", "lista priorizada\nde limpeza + alertas\npara a região", cor=VERDE, tam=9)
caixa(147, 6, 21, 14, "BI e pitch", "consulta de impacto:\ncríticos em risco\ncom chuva prevista", cor=VERDE, tam=9)

ax.text(2, 1.5, "Azul: fontes externas reais · Verde: componentes das outras disciplinas · Tracejado: etapa manual (Free Edition não conecta ao Airflow)",
        fontsize=8.2, color="#475569")
saida = RAIZ / "docs" / "arquitetura.png"
fig.savefig(saida, bbox_inches="tight", facecolor="white")
print(saida)
