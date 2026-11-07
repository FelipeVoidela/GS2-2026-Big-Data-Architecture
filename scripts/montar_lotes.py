"""
Separa os arquivos de chegada/ em lotes para enviar ao Volume do Databricks pela interface.

O Databricks Free Edition não se conecta ao Airflow, então os arquivos sobem à mão, em lotes de dias
de coleta diferentes (requisito 04). Este script copia, para cada lote, a mesma estrutura de pastas
que o Volume espera:

    lotes/lote_01/previsao/previsao_2026-11-06.json
    lotes/lote_01/pluviometros/...
    ...

Depois, no Databricks: Catalog > workspace > gs > Volumes > chegada > abra a pasta da fonte >
"Upload to this volume" e arraste os arquivos da pasta correspondente do lote.

Uso:
    python scripts/montar_lotes.py --lote 1 --datas 2026-11-06
    python scripts/montar_lotes.py --lote 2 --datas 2026-11-07
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
FONTES = {"previsao": "json", "pluviometros": "json", "areas_risco": "json", "sp156": "csv", "inspecoes": "json"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lote", type=int, required=True)
    ap.add_argument("--datas", nargs="+", required=True, help="datas de coleta (AAAA-MM-DD) que entram no lote")
    ap.add_argument("--chegada", default=str(RAIZ / "chegada"))
    a = ap.parse_args()

    destino = RAIZ / "lotes" / f"lote_{a.lote:02d}"
    total = 0
    for fonte, ext in FONTES.items():
        for ds in a.datas:
            origem = Path(a.chegada) / fonte / f"{fonte}_{ds}.{ext}"
            if not origem.exists():
                print(f"AVISO: {origem.name} não existe (a coleta desse dia rodou?)")
                continue
            (destino / fonte).mkdir(parents=True, exist_ok=True)
            shutil.copy2(origem, destino / fonte / origem.name)
            total += 1
    print(f"Lote {a.lote}: {total} arquivos em {destino}")
    for p in sorted(destino.rglob("*.*")):
        print("  ", p.relative_to(destino), f"{p.stat().st_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
