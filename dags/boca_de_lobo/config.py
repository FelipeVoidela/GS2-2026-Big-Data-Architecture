"""Configuração da coleta. Nada aqui é segredo: chaves e URLs sensíveis vêm de Variables do Airflow."""
from __future__ import annotations

import os
from pathlib import Path

# Pasta de pouso montada no container (docker-compose.yaml -> ./chegada:/opt/airflow/chegada)
CHEGADA = Path(os.environ.get("GS_CHEGADA", "/opt/airflow/chegada"))
REF = Path(__file__).resolve().parent / "ref"

# Município de São Paulo
COD_IBGE_SP = 3550308
UF = "SP"

# Grade de previsão do Open-Meteo sobre a cidade (passo de 0,1° ~ 11 km, a resolução real dos modelos).
GRADE_PASSO = 0.1
GRADE_LAT = (-24.0, -23.3)
GRADE_LON = (-46.9, -46.3)

# Nomes das Variables do Airflow (Admin > Variables). Nenhuma é obrigatória para o DAG rodar.
VAR_OPEN_METEO_APIKEY = "gs_open_meteo_apikey"   # só para o plano comercial do Open-Meteo
VAR_ALERTA_WEBHOOK = "gs_alerta_webhook_url"      # webhook (Teams/Slack/Discord) para notificação

# Endpoints públicos
OPEN_METEO_PUBLICO = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_COMERCIAL = "https://customer-api.open-meteo.com/v1/forecast"
CEMADEN_ACUMULADOS = "https://resources.cemaden.gov.br/graficos/interativo/getJson2.php"
GEOSAMPA_WFS = "http://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs"
GEOSAMPA_CAMADA = "geoportal:risco_hidrologico"
SP156_CKAN = "https://dados.prefeitura.sp.gov.br/api/3/action/package_show?id=dados-do-sp156"

# Serviços do SP156 ligados a drenagem e alagamento (o arquivo trimestral tem ~480 mil linhas de todos os temas)
SP156_ASSUNTOS = {
    "Drenagem de água de chuva",
    "Rios e Córregos",
}
SP156_SERVICOS_EXTRA = {
    "Solicitar limpeza da via pública após enchentes ou eventos",
    "Alagamento",
    "Rompimento de galeria de águas pluviais",
}

# Fontes esperadas por coleta: pasta, prefixo e extensão do arquivo em chegada/
FONTES = {
    "previsao": "json",
    "pluviometros": "json",
    "areas_risco": "json",
    "sp156": "csv",
    "inspecoes": "json",
}


def arquivo_destino(fonte: str, ds: str) -> Path:
    """chegada/<fonte>/<fonte>_<AAAA-MM-DD>.<ext> — um arquivo por coleta, com a data no nome."""
    return CHEGADA / fonte / f"{fonte}_{ds}.{FONTES[fonte]}"
