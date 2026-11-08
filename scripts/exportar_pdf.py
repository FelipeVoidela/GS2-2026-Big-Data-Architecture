"""
Exporta relatorio/Relatorio_BigData_BocaDeLobo.docx para PDF sem Word nem LibreOffice.

Caminho: .docx -> HTML (mammoth, imagens embutidas em base64) -> PDF (Chrome em modo headless).
É um conversor de reserva: se houver Word ou Google Docs à mão, o PDF exportado por eles fica mais fiel.

Uso:
    pip install mammoth
    python scripts/exportar_pdf.py            # gera relatorio/Relatorio_BigData_BocaDeLobo.pdf
    python scripts/exportar_pdf.py --html     # mantém também o HTML intermediário para conferência
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import mammoth

RAIZ = Path(__file__).resolve().parents[1]
DOCX = RAIZ / "relatorio" / "Relatorio_BigData_BocaDeLobo.docx"
PDF = DOCX.with_suffix(".pdf")

CHROMES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    "google-chrome", "chromium", "chromium-browser", "msedge",
]

# Estilos do .docx -> HTML (o que não está aqui vira <p>)
MAPA_ESTILOS = """
p[style-name='Caption'] => p.legenda:fresh
p[style-name='Title'] => h1.titulo:fresh
p[style-name='Subtitle'] => p.subtitulo:fresh
"""

CSS = """
@page { size: A4; margin: 2cm 1.8cm; }
body { font-family: Calibri, 'Segoe UI', Arial, sans-serif; font-size: 10.5pt; color: #222; line-height: 1.35; }
h1 { font-size: 16pt; color: #1f3864; margin: 18pt 0 8pt; page-break-after: avoid; }
h1.titulo { font-size: 24pt; text-align: center; margin-top: 120pt; }
.quebra { page-break-after: always; }
h2 { font-size: 13pt; color: #2e5597; margin: 14pt 0 6pt; }
h3 { font-size: 11.5pt; margin: 10pt 0 4pt; }
p { margin: 0 0 6pt; text-align: justify; }
p.subtitulo { text-align: center; color: #555; font-size: 12pt; }
p.legenda { text-align: center; font-size: 9pt; color: #555; font-style: italic; margin: 2pt 0 12pt; page-break-before: avoid; }
img { max-width: 100%; height: auto; display: block; margin: 8pt auto 2pt; page-break-inside: avoid; }
table { border-collapse: collapse; width: 100%; margin: 6pt 0 10pt; font-size: 9pt; page-break-inside: avoid; }
th, td { border: 1px solid #999; padding: 3pt 5pt; vertical-align: top; text-align: left; }
tr:first-child td, th { background: #dbe5f1; font-weight: bold; }
pre, code { font-family: Consolas, 'Courier New', monospace; font-size: 8.5pt; }
pre { background: #f4f4f4; padding: 6pt; border: 1px solid #ddd; white-space: pre-wrap; page-break-inside: avoid; }
a { color: #1f3864; }
"""


def achar_chrome() -> str:
    for c in CHROMES:
        if os.path.isabs(c) and Path(c).exists():
            return c
        achado = shutil.which(c)
        if achado:
            return achado
    sys.exit("Chrome/Edge não encontrado. Instale o Google Chrome ou exporte o PDF pelo Word/Google Docs.")


def docx_para_html(docx: Path) -> str:
    with open(docx, "rb") as fh:
        resultado = mammoth.convert_to_html(fh, style_map=MAPA_ESTILOS)
    for aviso in resultado.messages:
        print("  aviso:", aviso.message)
    corpo = resultado.value.replace("<h1>", "<div class='quebra'></div><h1>", 1)  # capa em página própria
    return (
        "<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'>"
        f"<title>{docx.stem}</title><style>{CSS}</style></head><body>{corpo}</body></html>"
    )


def html_para_pdf(html: Path, pdf: Path) -> None:
    chrome = achar_chrome()
    cmd = [
        chrome, "--headless=new", "--disable-gpu", "--no-sandbox", "--allow-file-access-from-files",
        "--no-pdf-header-footer", f"--print-to-pdf={pdf}", "--virtual-time-budget=20000", html.as_uri(),
    ]
    subprocess.run(cmd, check=True, capture_output=True, timeout=300)


def main() -> None:
    if not DOCX.exists():
        sys.exit(f"Não achei {DOCX}. Rode antes: python scripts/gerar_relatorio.py")
    manter_html = "--html" in sys.argv
    print(f"Convertendo {DOCX.name} -> HTML")
    html = docx_para_html(DOCX)
    destino_html = DOCX.with_suffix(".html") if manter_html else Path(tempfile.mkdtemp()) / "relatorio.html"
    destino_html.write_text(html, encoding="utf-8")
    print(f"Imprimindo em PDF com o Chrome -> {PDF}")
    html_para_pdf(destino_html, PDF)
    if not manter_html:
        shutil.rmtree(destino_html.parent, ignore_errors=True)
    print(f"Pronto: {PDF} ({PDF.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
