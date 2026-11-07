"""
Gera a lista de bocas de lobo do piloto (dags/boca_de_lobo/ref/bueiros_piloto.csv).

Por que existe: a prefeitura não publica o cadastro georreferenciado de bocas de lobo.
Para que o robô simulado inspecione lugares REAIS, usamos como ponto de partida os
endereços de pedidos reais de "limpeza de bueiros, boca de lobo" do SP156 e as áreas
de risco hidrológico do GeoSampa:

  * 60% dos pontos ficam dentro ou a até ~130 m de uma área de risco (H3 res 10 + vizinhança 1);
  * 40% ficam no restante da cidade, para o mapa ter contraste.

O número da casa NÃO é guardado (LGPD): só logradouro, distrito e subprefeitura, e a
coordenada é a do trecho de rua (infraestrutura pública), não a do solicitante.

Uso:
    python scripts/gerar_bueiros_piloto.py --sp156 caminho/arquivo-final-3-trim-26.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import urllib.request
from pathlib import Path

import h3

RAIZ = Path(__file__).resolve().parents[1]
SAIDA = RAIZ / "dags" / "boca_de_lobo" / "ref" / "bueiros_piloto.csv"
WFS = (
    "http://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs?service=WFS&version=2.0.0"
    "&request=GetFeature&outputFormat=application/json&srsName=EPSG:4326"
    "&typeNames=geoportal:risco_hidrologico"
)
SERVICO = "Solicitar limpeza de bueiros, boca de lobo e poços de visita"
TOTAL = 300
FRACAO_RISCO = 0.6


def celulas_de_risco() -> set[str]:
    with urllib.request.urlopen(WFS, timeout=120) as r:
        fc = json.load(r)
    celulas: set[str] = set()
    for f in fc["features"]:
        g = f["geometry"]
        celulas.update(h3.geo_to_cells(g, 10))
        for lon, lat in g["coordinates"][0]:  # vértices: garante polígonos pequenos
            celulas.add(h3.latlng_to_cell(lat, lon, 10))
    vizinhanca: set[str] = set()
    for c in celulas:
        vizinhanca.update(h3.grid_disk(c, 1))
    return vizinhanca


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sp156", required=True, help="CSV trimestral do SP156 (dados abertos)")
    args = ap.parse_args()

    risco = celulas_de_risco()
    candidatos: dict[str, dict] = {}
    with open(args.sp156, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("Serviço") != SERVICO or not row.get("Latitude"):
                continue
            try:
                lat, lon = float(row["Latitude"]), float(row["Longitude"])
            except ValueError:
                continue
            if not (-24.01 <= lat <= -23.35 and -46.83 <= lon <= -46.36):
                continue
            c11 = h3.latlng_to_cell(lat, lon, 11)  # 1 bueiro por célula de ~25 m
            if c11 in candidatos:
                continue
            candidatos[c11] = {
                "latitude": round(lat, 5),
                "longitude": round(lon, 5),
                "logradouro": row["Logradouro"].strip(),
                "distrito": row["Distrito"].strip(),
                "subprefeitura": row["Prefeitura Operacional"].strip(),
                "perto_risco": h3.latlng_to_cell(lat, lon, 10) in risco,
            }

    rnd = random.Random(42)
    perto = [c for c in candidatos.values() if c["perto_risco"]]
    longe = [c for c in candidatos.values() if not c["perto_risco"]]
    n_perto = min(len(perto), int(TOTAL * FRACAO_RISCO))
    escolhidos = rnd.sample(perto, n_perto) + rnd.sample(longe, TOTAL - n_perto)
    escolhidos.sort(key=lambda c: (c["subprefeitura"], c["logradouro"]))

    SAIDA.parent.mkdir(parents=True, exist_ok=True)
    with open(SAIDA, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["id_bueiro", "latitude", "longitude", "logradouro", "distrito",
                    "subprefeitura", "profundidade_nominal_cm", "robo_id", "perto_area_risco"])
        for i, c in enumerate(escolhidos, start=1):
            w.writerow([f"BL-{i:04d}", c["latitude"], c["longitude"], c["logradouro"],
                        c["distrito"], c["subprefeitura"], rnd.choice([40, 50, 60, 70, 80]),
                        f"ROBO-{(i % 4) + 1:02d}", int(c["perto_risco"])])
    print(f"{len(escolhidos)} bueiros gravados em {SAIDA} ({n_perto} perto de área de risco)")


if __name__ == "__main__":
    main()
