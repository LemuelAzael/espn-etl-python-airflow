from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Configuración base del DAG: propietario, fecha de inicio y reintentos
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 15),
    "retries": 1,
}

# Definición del DAG principal
with DAG(
    dag_id="etl_espn",                  # Identificador único del DAG
    default_args=default_args,          # Argumentos por defecto
    description="ETL secuencial usando scripts Python",
    schedule_interval=None,             # No programado; ejecución manual
    catchup=False,                      # No ejecutar ejecuciones pasadas
) as dag:

    # Tarea de transformación de datos usando un script Python
    extrac_transform_task = BashOperator(
        task_id="transform_data",  
        bash_command="python /opt/airflow/scripts/extract_transform.py"
    )

    # Tarea de carga y disponibilización de datos usando un script Python
    load_task = BashOperator(
        task_id="load_data",
        bash_command="python /opt/airflow/scripts/load.py"
    )

    # Definición del flujo de ejecución: transformación antes de la carga
    extrac_transform_task >> load_task


