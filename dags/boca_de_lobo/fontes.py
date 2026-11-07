"""
Funções de coleta chamadas pelo DAG gs_boca_de_lobo_coleta.

Regras comuns a todas as fontes:
  * um arquivo por coleta em chegada/<fonte>/<fonte>_<ds>.<ext> (data da execução no nome);
  * gravação atômica (.tmp + rename): se a tarefa cair no meio, não sobra arquivo pela metade;
  * rodar de novo a mesma data sobrescreve o mesmo arquivo, sem criar duplicata (idempotente);
  * chaves e URLs sensíveis só via Variables do Airflow, nunca no código.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import date, timedelta
from pathlib import Path

import requests

from boca_de_lobo import config
from boca_de_lobo.simulador_robo import gerar_inspecoes

log = logging.getLogger(__name__)
TIMEOUT = 60
CABECALHO_HTTP = {"User-Agent": "gs-boca-de-lobo/1.0 (projeto academico FIAP)"}


def _variable(nome: str) -> str | None:
    """Lê uma Variable do Airflow; devolve None se não existir (todas são opcionais)."""
    from airflow.sdk import Variable

    try:
        valor = Variable.get(nome, default=None)
    except Exception:  # noqa: BLE001 - fora do Airflow (testes locais)
        return None
    return valor or None


def _gravar(destino: Path, conteudo: str) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".tmp")
    tmp.write_text(conteudo, encoding="utf-8")
    tmp.replace(destino)
    log.info("Gravado %s (%d bytes)", destino, destino.stat().st_size)
    return destino


def _grade() -> list[tuple[float, float]]:
    pontos = []
    passo = config.GRADE_PASSO
    n_lat = round((config.GRADE_LAT[1] - config.GRADE_LAT[0]) / passo) + 1
    n_lon = round((config.GRADE_LON[1] - config.GRADE_LON[0]) / passo) + 1
    for i in range(n_lat):
        for j in range(n_lon):
            pontos.append((round(config.GRADE_LAT[0] + i * passo, 2), round(config.GRADE_LON[0] + j * passo, 2)))
    return pontos


# ---------------------------------------------------------------- 1. Open-Meteo (API externa)
def coletar_previsao(ds: str, **_) -> str:
    """Previsão horária de chuva (48 h a partir de ds) para a grade de pontos sobre São Paulo."""
    grade = _grade()
    params = {
        "latitude": ",".join(str(p[0]) for p in grade),
        "longitude": ",".join(str(p[1]) for p in grade),
        "hourly": "precipitation,precipitation_probability",
        "timezone": "America/Sao_Paulo",
        "start_date": ds,
        "end_date": (date.fromisoformat(ds) + timedelta(days=1)).isoformat(),
    }
    url = config.OPEN_METEO_PUBLICO
    apikey = _variable(config.VAR_OPEN_METEO_APIKEY)
    if apikey:  # plano comercial: a chave vem da Variable, nunca do código
        url, params["apikey"] = config.OPEN_METEO_COMERCIAL, apikey
    r = requests.get(url, params=params, timeout=TIMEOUT, headers=CABECALHO_HTTP)
    r.raise_for_status()
    locais = r.json()
    if isinstance(locais, dict):
        locais = [locais]
    # O Open-Meteo devolve a coordenada do modelo, não a pedida: guardamos a da grade
    # para a Prata ligar cada bueiro à sua célula de previsão.
    for (lat, lon), loc in zip(grade, locais):
        loc["lat_grade"], loc["lon_grade"] = lat, lon
    return str(_gravar(config.arquivo_destino("previsao", ds), json.dumps(locais, ensure_ascii=False)))


# ---------------------------------------------------------------- 2. Cemaden (dados abertos)
def coletar_pluviometros(ds: str, **_) -> str:
    """Acumulados de chuva (1 h a 96 h) dos pluviômetros do Cemaden no estado de SP."""
    r = requests.get(config.CEMADEN_ACUMULADOS, params={"uf": config.UF}, timeout=TIMEOUT, headers=CABECALHO_HTTP)
    r.raise_for_status()
    estacoes = r.json()
    return str(_gravar(config.arquivo_destino("pluviometros", ds), json.dumps(estacoes, ensure_ascii=False)))


# ---------------------------------------------------------------- 3. GeoSampa (WFS da prefeitura)
def coletar_areas_risco(ds: str, **_) -> str:
    """Setores de risco hidrológico (enchente, alagamento, enxurrada) do GeoSampa.

    Gravado como JSON Lines, um setor por linha, com a geometria serializada em texto
    GeoJSON: assim o Auto Loader não precisa inferir arrays de coordenadas aninhados.
    """
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": config.GEOSAMPA_CAMADA, "outputFormat": "application/json", "srsName": "EPSG:4326",
    }
    r = requests.get(config.GEOSAMPA_WFS, params=params, timeout=180, headers=CABECALHO_HTTP)
    r.raise_for_status()
    linhas = []
    for f in r.json()["features"]:
        props = dict(f["properties"])
        props["feature_id"] = f.get("id")
        props["geometria_geojson"] = json.dumps(f["geometry"], separators=(",", ":"))
        linhas.append(json.dumps(props, ensure_ascii=False))
    return str(_gravar(config.arquivo_destino("areas_risco", ds), "\n".join(linhas) + "\n"))


# ---------------------------------------------------------------- 4. SP156 (dados abertos)
def _url_sp156_mais_recente() -> tuple[str, str]:
    r = requests.get(config.SP156_CKAN, timeout=TIMEOUT, headers=CABECALHO_HTTP)
    r.raise_for_status()
    recursos = [
        x for x in r.json()["result"]["resources"]
        if "csv" in (x.get("format") or "").lower() and "Dados do SP156" in (x.get("name") or "")
    ]
    recursos.sort(key=lambda x: x.get("created") or "", reverse=True)
    return recursos[0]["url"], recursos[0]["id"]


def _normalizar(nome: str) -> str:
    import unicodedata

    s = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode().lower()
    return "_".join("".join(c if c.isalnum() else " " for c in s).split())


def coletar_sp156(ds: str, **_) -> str:
    """Solicitações do SP156 sobre bueiro, drenagem e alagamento (arquivo trimestral mais recente).

    O arquivo completo tem ~100 MB e todos os temas da cidade. Ele fica em cache por recurso
    (chegada/_cache) e só as linhas de drenagem/alagamento vão para a pasta de pouso, com o
    cabeçalho em snake_case sem acento (o Delta não aceita espaço em nome de coluna).
    """
    url, recurso_id = _url_sp156_mais_recente()
    cache = config.CHEGADA / "_cache" / f"sp156_{recurso_id}.csv"
    if not cache.exists():
        cache.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(url, stream=True, timeout=600, headers=CABECALHO_HTTP) as r:
            r.raise_for_status()
            tmp = cache.with_suffix(".tmp")
            with open(tmp, "wb") as fh:
                for bloco in r.iter_content(1 << 20):
                    fh.write(bloco)
            tmp.replace(cache)
    bruto = cache.read_bytes()
    try:
        texto = bruto.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = bruto.decode("latin-1")
    leitor = csv.reader(io.StringIO(texto))
    cabecalho = next(leitor)
    idx_assunto, idx_servico = cabecalho.index("Assunto"), cabecalho.index("Serviço")
    saida = io.StringIO()
    w = csv.writer(saida)
    w.writerow([_normalizar(c) for c in cabecalho] + ["recurso_ckan"])
    n = 0
    for linha in leitor:
        if len(linha) != len(cabecalho):
            continue
        if linha[idx_assunto] in config.SP156_ASSUNTOS or linha[idx_servico] in config.SP156_SERVICOS_EXTRA:
            w.writerow(linha + [recurso_id])
            n += 1
    log.info("SP156: %d solicitações de drenagem/alagamento", n)
    return str(_gravar(config.arquivo_destino("sp156", ds), saida.getvalue()))


# ---------------------------------------------------------------- 5. Robô (Maker Lab + Visão)
def coletar_inspecoes(ds: str, **_) -> str:
    """Inspeções do robô do dia (JSON Lines). Hoje vêm do simulador; depois, da API do robô."""
    registros = gerar_inspecoes(ds, config.REF / "bueiros_piloto.csv")
    conteudo = "\n".join(json.dumps(x, ensure_ascii=False) for x in registros) + "\n"
    return str(_gravar(config.arquivo_destino("inspecoes", ds), conteudo))


# ---------------------------------------------------------------- Conferência e notificação
def _contar(caminho: Path) -> int:
    if caminho.suffix == ".csv":
        with open(caminho, encoding="utf-8") as fh:
            return max(0, sum(1 for _ in fh) - 1)
    texto = caminho.read_text(encoding="utf-8").strip()
    if texto.startswith("["):
        return len(json.loads(texto))
    return sum(1 for linha in texto.splitlines() if linha.strip() and json.loads(linha))


def conferir_chegada(ds: str, **_) -> dict:
    """Confere se cada fonte gerou o arquivo do dia, não vazio e legível. Falha se faltar algum."""
    resumo, faltando = {}, []
    for fonte in config.FONTES:
        caminho = config.arquivo_destino(fonte, ds)
        if not caminho.exists() or caminho.stat().st_size == 0:
            faltando.append(fonte)
            continue
        try:
            resumo[fonte] = _contar(caminho)
        except (ValueError, json.JSONDecodeError) as exc:
            faltando.append(f"{fonte} (ilegível: {exc})")
            continue
        if resumo[fonte] == 0:
            faltando.append(f"{fonte} (0 registros)")
    log.info("Conferência %s: %s", ds, resumo)
    if faltando:
        raise FileNotFoundError(f"Coleta {ds} incompleta. Faltando: {', '.join(faltando)}")
    return resumo


def _enviar_webhook(texto: str) -> None:
    url = _variable(config.VAR_ALERTA_WEBHOOK)
    if not url:
        log.warning("Variable %s não definida: notificação só no log. %s", config.VAR_ALERTA_WEBHOOK, texto)
        return
    try:
        requests.post(url, json={"text": texto, "content": texto}, timeout=15)
    except requests.RequestException as exc:  # notificação nunca derruba o pipeline
        log.error("Falha ao notificar: %s", exc)


def notificar(ds: str, ti=None, **_) -> None:
    resumo = ti.xcom_pull(task_ids="conferir_chegada") if ti else None
    if resumo:
        partes = ", ".join(f"{k}={v}" for k, v in resumo.items())
        _enviar_webhook(f"[Boca de Lobo] Coleta {ds} concluída: {partes}. Pronta para subir ao Volume.")
    else:
        _enviar_webhook(f"[Boca de Lobo] Coleta {ds} terminou SEM conferência OK. Verifique o Airflow.")
        # notificar é a última tarefa: se ela passasse, a execução ficaria verde mesmo com falha.
        raise RuntimeError(f"Coleta {ds} incompleta: alerta enviado, execução marcada como falha.")


def alerta_falha(context) -> None:
    """on_failure_callback: avisa na hora em que qualquer fonte cai (falha silenciosa é o pior cenário)."""
    ti = context.get("task_instance")
    _enviar_webhook(
        f"[Boca de Lobo] FALHA na tarefa {getattr(ti, 'task_id', '?')} "
        f"da coleta {context.get('ds')}. Tentativas esgotadas."
    )
