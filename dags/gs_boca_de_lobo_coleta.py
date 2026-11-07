"""
DAG gs_boca_de_lobo_coleta — componente Dados do MVP "Boca de Lobo".

Todo dia às 06h (antes do bot de RPA, que roda às 07h), coleta em paralelo as cinco fontes
e grava um arquivo por fonte em chegada/<fonte>/<fonte>_<data>.<ext>:

    inicio >> [previsao, pluviometros, areas_risco, sp156, inspecoes] >> conferir_chegada >> notificar

Depois, os arquivos de chegada/ sobem ao Volume do Databricks pela interface, em lotes
(o Databricks Free Edition não se conecta ao Airflow).
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from boca_de_lobo import fontes

PADRAO = {
    "owner": "equipe-boca-de-lobo",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": fontes.alerta_falha,
}

with DAG(
    dag_id="gs_boca_de_lobo_coleta",
    description="Coleta diária: previsão de chuva, pluviômetros, áreas de risco, SP156 e inspeções do robô",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=PADRAO,
    tags=["gs", "boca-de-lobo", "big-data"],
    doc_md=__doc__,
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    coletas = [
        PythonOperator(task_id="coletar_previsao_open_meteo", python_callable=fontes.coletar_previsao),
        PythonOperator(task_id="coletar_pluviometros_cemaden", python_callable=fontes.coletar_pluviometros),
        PythonOperator(task_id="coletar_areas_risco_geosampa", python_callable=fontes.coletar_areas_risco),
        PythonOperator(
            task_id="coletar_solicitacoes_sp156",
            python_callable=fontes.coletar_sp156,
            execution_timeout=timedelta(minutes=20),
        ),
        PythonOperator(task_id="coletar_inspecoes_robo", python_callable=fontes.coletar_inspecoes),
    ]

    conferir = PythonOperator(task_id="conferir_chegada", python_callable=fontes.conferir_chegada, retries=0)

    # all_done: avisa mesmo quando alguma coleta falhou (aí a mensagem diz que a conferência não passou)
    notificar = PythonOperator(
        task_id="notificar", python_callable=fontes.notificar, trigger_rule="all_done", retries=0
    )

    inicio >> coletas >> conferir >> notificar
