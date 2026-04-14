from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sales_quality_etl',
    default_args=default_args,
    description='Pipeline ETL avec détection ML et correction LLM',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 4, 13),
    catchup=False,
) as dag:

    # Remplace le chemin par le chemin absolu de ton environnement si nécessaire
    run_main_pipeline = BashOperator(
        task_id='run_etl_pipeline',
        bash_command='cd "/path/to/project/System ETL" && python src/main.py',
    )
