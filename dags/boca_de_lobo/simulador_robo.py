"""
Simulador das inspeções do robô (componente Maker Lab + Visão Computacional).

Por que simulado: o carrinho com sensor ultrassônico e o YOLO ainda estão sendo construídos
pelas disciplinas de Maker Lab e Visão. Enquanto isso, este módulo produz exatamente o
contrato JSON que o robô vai enviar, para o pipeline de dados ser construído em paralelo.
Quando o robô estiver no ar, a tarefa `coletar_inspecoes` passa a ler a API do robô e
este arquivo deixa de ser usado. Ver README, seção "Dados simulados".

Como a geração é feita (justificativa):
  * Os pontos são reais: endereços de pedidos de limpeza de boca de lobo do SP156
    (ref/bueiros_piloto.csv), sem número de casa.
  * Cada bueiro acumula lixo e folhas em ritmo próprio (2 a 14% ao dia, mais rápido perto
    de área de risco, que costuma ser área baixa) e é limpo quando entope: dente de serra.
  * A classificação livre/atenção/crítico segue a regra da IA embarcada (obstrução >= 70%
    crítico, >= 40% atenção). A classe do YOLO segue a obstrução, com confiança 0,55-0,97.
  * Para testar as regras de qualidade da Prata, ~6% dos registros vêm com defeito
    proposital: leitura nula, profundidade fora da faixa, confiança > 1, classe desconhecida,
    GPS zerado e envio duplicado.
  * É determinístico: a mesma data gera o mesmo arquivo (coleta idempotente).
  * Operadores são FICTÍCIOS (nomes inventados, domínio de exemplo). Nenhum dado pessoal real.
"""
from __future__ import annotations

import csv
import hashlib
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

FUSO = timezone(timedelta(hours=-3))
CLASSES_YOLO = ["livre", "parcialmente_obstruido", "entupido", "lixo_sarjeta"]
OPERADORES = [
    ("Operador Ficticio Ana Lima", "OP1001"),
    ("Operador Ficticio Bruno Reis", "OP1002"),
    ("Operador Ficticio Carla Souza", "OP1003"),
    ("Operador Ficticio Diego Alves", "OP1004"),
    ("Operador Ficticio Elisa Rocha", "OP1005"),
    ("Operador Ficticio Fabio Nunes", "OP1006"),
    ("Operador Ficticio Gabi Costa", "OP1007"),
    ("Operador Ficticio Heitor Melo", "OP1008"),
]


def _semente(*partes: str) -> int:
    return int(hashlib.sha256("|".join(partes).encode()).hexdigest()[:12], 16)


def carregar_bueiros(caminho: Path) -> list[dict]:
    with open(caminho, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _obstrucao(id_bueiro: str, perto_risco: bool, dia: date) -> float:
    r = random.Random(_semente(id_bueiro, "perfil"))
    taxa = r.uniform(0.02, 0.11) + (0.03 if perto_risco else 0.0)
    fase = r.uniform(0, 1.2)
    t = (dia - date(2026, 1, 1)).days
    ciclo = (fase + taxa * t) % 1.2           # passa de 1,0 = entupido até a limpeza
    ruido = random.Random(_semente(id_bueiro, dia.isoformat())).uniform(-0.05, 0.05)
    return max(0.0, min(1.0, ciclo + ruido))


def _status(obs: float) -> str:
    if obs >= 0.70:
        return "critico"
    if obs >= 0.40:
        return "atencao"
    return "livre"


def _classe(obs: float, r: random.Random) -> str:
    if obs >= 0.75:
        return "entupido"
    if obs >= 0.40:
        return "parcialmente_obstruido"
    return "lixo_sarjeta" if r.random() < 0.15 else "livre"


def gerar_inspecoes(ds: str, caminho_bueiros: Path) -> list[dict]:
    dia = date.fromisoformat(ds)
    registros: list[dict] = []
    for b in carregar_bueiros(caminho_bueiros):
        r = random.Random(_semente(b["id_bueiro"], ds, "insp"))
        if r.random() > 0.70:                 # o robô passa em ~70% dos pontos por dia
            continue
        nominal = float(b["profundidade_nominal_cm"])
        obs = _obstrucao(b["id_bueiro"], b.get("perto_area_risco") == "1", dia)
        prof_livre = round(max(0.0, nominal * (1 - obs) + r.uniform(-1.5, 1.5)), 1)
        nome, matricula = OPERADORES[_semente(b["robo_id"], ds) % len(OPERADORES)]
        hora = datetime(dia.year, dia.month, dia.day, 4, 0, tzinfo=FUSO) + timedelta(minutes=r.randint(0, 110))
        reg = {
            "id_inspecao": f"INS-{dia:%Y%m%d}-{b['id_bueiro']}",
            "id_bueiro": b["id_bueiro"],
            "robo_id": b["robo_id"],
            "inspecionado_em": hora.isoformat(),
            "localizacao": {
                "lat": round(float(b["latitude"]) + r.uniform(-0.00003, 0.00003), 6),
                "lon": round(float(b["longitude"]) + r.uniform(-0.00003, 0.00003), 6),
            },
            "endereco": {  # vem do plano de rota do robô (cadastro do ponto), sem número de casa
                "logradouro": b["logradouro"],
                "distrito": b["distrito"],
                "subprefeitura": b["subprefeitura"],
            },
            "sensores": {
                "ultrassom_profundidade_livre_cm": prof_livre,
                "profundidade_nominal_cm": nominal,
                "nivel_agua_cm": round(max(0.0, r.gauss(0.5, 1.0)), 1),
                "chuva_detectada": r.random() < 0.1,
            },
            "visao": {
                "modelo": "yolov8n-bocadelobo-v1",
                "classe": _classe(obs, r),
                "confianca": round(r.uniform(0.55, 0.97), 3),
                "foto_id": hashlib.md5(f"{b['id_bueiro']}{ds}".encode()).hexdigest()[:16],
            },
            "ia_embarcada": {"status": _status(obs), "obstrucao_pct": round(obs * 100, 1)},
            "operador": {
                "nome": nome,
                "matricula": matricula,
                "email": f"{matricula.lower()}@limpeza.exemplo.org",
            },
        }
        # Defeitos propositais (testam as regras de qualidade da Prata)
        d = r.random()
        if d < 0.015:
            reg["sensores"]["ultrassom_profundidade_livre_cm"] = None
        elif d < 0.025:
            reg["sensores"]["ultrassom_profundidade_livre_cm"] = round(r.choice([-12.0, nominal + 35]), 1)
        elif d < 0.035:
            reg["visao"]["confianca"] = round(r.uniform(1.2, 9.9), 2)
        elif d < 0.042:
            reg["visao"]["classe"] = "desconhecido"
        elif d < 0.047:
            reg["localizacao"] = {"lat": 0.0, "lon": 0.0}
        registros.append(reg)
        if r.random() < 0.02:                 # reenvio duplicado (mesmo id_inspecao)
            registros.append(json.loads(json.dumps(reg)))
    return registros
