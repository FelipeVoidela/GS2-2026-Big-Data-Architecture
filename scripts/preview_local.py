"""
Prévia local da camada Ouro (sem Databricks) para testar a tela do MVP antes do lakehouse estar pronto.

Reproduz em pandas + h3 as MESMAS regras dos notebooks 02 e 03 (qualidade, H3 res 10 + vizinhança,
grade de previsão de 0,1°, janela de 120 dias do SP156, score e nível de alerta) e grava
mvp/dados/mapa_cidadao.json no mesmo formato que o notebook 03 exporta de `ouro_vw_mapa_cidadao`.

A fonte oficial do MVP é sempre a view da Ouro no Databricks. Este script serve para desenvolver
a tela e para conferir se os números da Ouro batem.

Uso:  python scripts/preview_local.py [--chegada chegada] [--limiar 10]
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import h3
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
LAT, LON = (-24.10, -23.20), (-47.00, -46.20)
CLASSES = {"livre", "parcialmente_obstruido", "entupido", "lixo_sarjeta"}
PONTOS_GRAU = {4: 30, 3: 24, 2: 16, 1: 8}


def ler_jsonl(pasta: Path, prefixo: str) -> pd.DataFrame:
    linhas = []
    for arq in sorted(pasta.glob(f"{prefixo}_*.json")):
        data = arq.stem.split("_")[-1]
        for l in arq.read_text(encoding="utf-8").splitlines():
            if l.strip():
                d = json.loads(l)
                d["data_coleta"] = data
                linhas.append(d)
    return pd.json_normalize(linhas)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chegada", default=str(RAIZ / "chegada"))
    ap.add_argument("--limiar", type=float, default=10.0)
    ap.add_argument("--janela", type=int, default=120)
    a = ap.parse_args()
    ch = Path(a.chegada)

    # ---------- Prata: inspeções (3 regras + dedup)
    ins = ler_jsonl(ch / "inspecoes", "inspecoes")
    p = pd.to_numeric(ins["sensores.ultrassom_profundidade_livre_cm"], errors="coerce")
    n = pd.to_numeric(ins["sensores.profundidade_nominal_cm"], errors="coerce")
    c = pd.to_numeric(ins["visao.confianca"], errors="coerce")
    la, lo = ins["localizacao.lat"].astype(float), ins["localizacao.lon"].astype(float)
    r1 = p.between(0, n)
    r2 = ins["visao.classe"].isin(CLASSES) & c.between(0, 1)
    r3 = la.between(*LAT) & lo.between(*LON)
    print(f"Inspeções: {len(ins)} | INSP_01 barrou {(~r1).sum()} | INSP_02 {(~r2).sum()} | INSP_03 {(~r3).sum()}")
    ins = ins[r1 & r2 & r3].copy()
    antes = len(ins)
    ins = ins.sort_values("data_coleta").drop_duplicates("id_inspecao", keep="last")
    print(f"  DEDUP removeu {antes - len(ins)}; válidas: {len(ins)}")
    ins["data_inspecao"] = ins["inspecionado_em"].str[:10]

    # ---------- Áreas de risco em H3
    ultima_area = sorted((ch / "areas_risco").glob("areas_risco_*.json"))[-1]
    areas = [json.loads(l) for l in ultima_area.read_text(encoding="utf-8").splitlines() if l.strip()]
    cel: dict[str, tuple[int, int, int]] = {}  # h3 -> (anel, grau, id_area)
    info_area = {}
    for ar in areas:
        g = json.loads(ar["geometria_geojson"])
        grau = int(ar["tx_grau_risco_hidrologico"].strip()[1:])
        ida = ar["cd_identificador_risco_hidrologico"]
        info_area[ida] = ar
        dentro = set(h3.geo_to_cells(g, 10)) | {h3.latlng_to_cell(y, x, 10) for x, y in g["coordinates"][0]}
        for c0 in dentro:
            for c1 in h3.grid_disk(c0, 1):
                anel = 0 if c1 in dentro else 1
                atual = cel.get(c1)
                if atual is None or (anel, -grau) < (atual[0], -atual[1]):
                    cel[c1] = (anel, grau, ida)

    # ---------- Dimensão bueiro
    ult = ins.sort_values("inspecionado_em").groupby("id_bueiro").tail(1)
    dim = pd.DataFrame({
        "id_bueiro": ult["id_bueiro"], "latitude": ult["localizacao.lat"].round(5),
        "longitude": ult["localizacao.lon"].round(5), "logradouro": ult["endereco.logradouro"].str.title(),
        "distrito": ult["endereco.distrito"], "subprefeitura": ult["endereco.subprefeitura"],
    })
    dim["h3_10"] = [h3.latlng_to_cell(y, x, 10) for y, x in zip(dim.latitude, dim.longitude)]
    dim["h3_9"] = [h3.latlng_to_cell(y, x, 9) for y, x in zip(dim.latitude, dim.longitude)]
    dim["celula"] = [f"{round(y * 10) / 10:.1f}_{round(x * 10) / 10:.1f}" for y, x in zip(dim.latitude, dim.longitude)]
    risco = dim["h3_10"].map(cel)
    dim["situacao_risco"] = risco.map(lambda t: "fora" if not isinstance(t, tuple) else ("dentro" if t[0] == 0 else "vizinho"))
    dim["grau_num"] = risco.map(lambda t: t[1] if isinstance(t, tuple) else 0)
    dim["grau_risco"] = dim["grau_num"].map(lambda g: f"R{g}" if g else None)
    dim["id_area"] = risco.map(lambda t: t[2] if isinstance(t, tuple) else None)

    # ---------- Previsão: soma das 24 h a partir das 06h do dia da coleta
    prev = {}
    for arq in sorted((ch / "previsao").glob("previsao_*.json")):
        ds = arq.stem.split("_")[-1]
        ini = datetime.fromisoformat(ds) + timedelta(hours=6)
        for loc in json.loads(arq.read_text(encoding="utf-8")):
            cel_id = f"{float(loc['lat_grade']):.1f}_{float(loc['lon_grade']):.1f}"
            h = loc["hourly"]
            tot, pmax = 0.0, 0
            for t, mm, pr in zip(h["time"], h["precipitation"], h["precipitation_probability"]):
                if ini <= datetime.fromisoformat(t) < ini + timedelta(hours=24) and mm is not None:
                    tot += mm
                    pmax = max(pmax, pr or 0)
            prev[(cel_id, ds)] = (round(tot, 1), pmax)

    # ---------- SP156: pedidos de limpeza na vizinhança (h3 res 9, anel 1), janela de N dias
    sp = pd.read_csv(sorted((ch / "sp156").glob("sp156_*.csv"))[-1], dtype=str)
    sp = sp[sp["servico"].str.contains("bueiro|boca de lobo", case=False, na=False) | (sp["assunto"] == "Defesa Civil")]
    sp = sp.dropna(subset=["latitude", "longitude"])
    sp["h3_9"] = [h3.latlng_to_cell(float(y), float(x), 9) for y, x in zip(sp.latitude, sp.longitude)]
    sp["abertura"] = pd.to_datetime(sp["data_de_abertura"], format="%m/%d/%Y", errors="coerce")

    # ---------- Fato do dia mais recente
    ref = ins["data_inspecao"].max()
    hoje = ins[ins["data_inspecao"] == ref].merge(dim, on="id_bueiro", suffixes=("", "_d"))
    lim_ini = pd.Timestamp(ref) - pd.Timedelta(days=a.janela)
    sp_j = sp[(sp.abertura >= lim_ini) & (sp.abertura <= pd.Timestamp(ref))]
    cont9 = sp_j.groupby("h3_9").size().to_dict()

    pontos = []
    for _, r in hoje.iterrows():
        chuva, prob = prev.get((r["celula"], ref), (None, None))
        pedidos = sum(cont9.get(v, 0) for v in h3.grid_disk(r["h3_9"], 1))
        st = r["ia_embarcada.status"]
        em_risco = r["situacao_risco"] in ("dentro", "vizinho")
        acima = (chuva or 0) >= a.limiar
        score = round((50 if st == "critico" else 25 if st == "atencao" else 0)
                      + {"dentro": 1.0, "vizinho": 0.5}.get(r["situacao_risco"], 0) * PONTOS_GRAU.get(r["grau_num"], 0)
                      + min(chuva or 0, 50) / 50 * 15 + min(pedidos, 5))
        if st == "critico" and em_risco and acima:
            nivel, rec = "EVITE A AREA", "Bueiro entupido em área que alaga e chuva forte prevista. Evite passar e não deixe o carro na rua."
        elif st == "critico" or (st == "atencao" and em_risco and acima):
            nivel = "ATENCAO"
            rec = "Bueiro entupido. Limpeza solicitada à equipe da região." if st == "critico" else "Bueiro parcialmente obstruído. Em observação."
        else:
            nivel = "NORMAL"
            rec = "Bueiro parcialmente obstruído. Em observação." if st == "atencao" else "Bueiro livre na última inspeção."
        pontos.append({
            "id_bueiro": r["id_bueiro"], "latitude": r["latitude"], "longitude": r["longitude"],
            "logradouro": r["logradouro"], "distrito": r["distrito"], "subprefeitura": r["subprefeitura"],
            "status": st, "obstrucao_pct": r["ia_embarcada.obstrucao_pct"], "classe_visao": r["visao.classe"],
            "nivel_alerta": nivel, "recomendacao": rec, "chuva_prevista_24h_mm": chuva, "prob_chuva_max_pct": prob,
            "situacao_risco": r["situacao_risco"], "grau_risco": None if pd.isna(r["grau_risco"]) else r["grau_risco"], "score_prioridade": int(score),
            "data_ultima_inspecao": ref, "_id_area": None if pd.isna(r["id_area"]) else r["id_area"],
        })

    alvo = [x for x in pontos if x["status"] == "critico" and x["situacao_risco"] != "fora"
            and (x["chuva_prevista_24h_mm"] or 0) >= a.limiar]
    ids = {x["_id_area"] for x in alvo}
    mor = sum(info_area[i]["qt_moradia"] or 0 for i in ids)
    print(f"\nData de referência {ref} | limiar {a.limiar} mm/24h")
    print(f"  inspecionados {len(pontos)} | críticos {sum(x['status'] == 'critico' for x in pontos)}"
          f" | críticos em área de risco {sum(x['status'] == 'critico' and x['situacao_risco'] != 'fora' for x in pontos)}"
          f" | críticos em risco com chuva >= limiar {len(alvo)}")
    print(f"  áreas afetadas {len(ids)} | moradias {mor} | pessoas estimadas {round(mor * 2.79)}")

    for x in pontos:
        x.pop("_id_area")
    saida = RAIZ / "mvp" / "dados" / "mapa_cidadao.json"
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(json.dumps({
        "gerado_em": datetime.now().isoformat(timespec="seconds"),
        "fonte": "previa local (scripts/preview_local.py) - substituir pelo export de workspace.gs.ouro_vw_mapa_cidadao",
        "limiar_chuva_mm": a.limiar, "pontos": pontos,
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\n{len(pontos)} pontos gravados em {saida}")


if __name__ == "__main__":
    main()
