"""
Copia um pouco de cada fonte de chegada/ para amostras/ (vai para o Git).

* Open-Meteo, Cemaden, GeoSampa: dados públicos, sem dado pessoal -> primeiros registros.
* Robô: operadores já são fictícios no simulador.
* SP156: mesmo sendo dado aberto, número da casa e CEP saem mascarados na amostra (LGPD).

Uso: python scripts/gerar_amostras.py --data 2026-11-07
"""
from __future__ import annotations

import argparse
import csv
import io
import json
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--n", type=int, default=20)
    a = ap.parse_args()
    ch, out = RAIZ / "chegada", RAIZ / "amostras"
    out.mkdir(exist_ok=True)

    prev = json.loads((ch / "previsao" / f"previsao_{a.data}.json").read_text(encoding="utf-8"))
    (out / f"previsao_{a.data}_amostra.json").write_text(json.dumps(prev[:3], ensure_ascii=False, indent=1), encoding="utf-8")

    pluv = json.loads((ch / "pluviometros" / f"pluviometros_{a.data}.json").read_text(encoding="utf-8"))
    capital = [x for x in pluv if x.get("codibge") == 3550308][: a.n]
    (out / f"pluviometros_{a.data}_amostra.json").write_text(json.dumps(capital, ensure_ascii=False, indent=1), encoding="utf-8")

    for fonte, n in (("areas_risco", 5), ("inspecoes", a.n)):
        linhas = (ch / fonte / f"{fonte}_{a.data}.json").read_text(encoding="utf-8").splitlines()[:n]
        (out / f"{fonte}_{a.data}_amostra.json").write_text("\n".join(linhas) + "\n", encoding="utf-8")

    with open(ch / "sp156" / f"sp156_{a.data}.csv", encoding="utf-8", newline="") as fh:
        leitor = csv.DictReader(fh)
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=leitor.fieldnames)
        w.writeheader()
        for i, row in enumerate(leitor):
            if i >= a.n:
                break
            if row["numero"].isdigit():
                base = int(row["numero"]) // 100 * 100
                row["numero"] = f"{base}-{base + 99}"
            row["cep"] = (row["cep"][:5] + "-***") if row["cep"] else ""
            if row["latitude"]:
                row["latitude"] = f"{float(row['latitude']):.3f}"
                row["longitude"] = f"{float(row['longitude']):.3f}"
            w.writerow(row)
    (out / f"sp156_{a.data}_amostra.csv").write_text(buf.getvalue(), encoding="utf-8")
    for p in sorted(out.iterdir()):
        print(p.name, f"{p.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
