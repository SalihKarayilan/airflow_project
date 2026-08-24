from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='dbt_dwh_daily_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 8, 20),
    schedule_interval='@daily',
    catchup=False,
    tags=['dwh', 'dbt', 'postgres']
) as dag:

    # 1. Görev: Sanal ortam kur, dbt'yi yükle ve modelleri çalıştır
    run_script = """
    python -m venv /opt/airflow/dbt_venv
    source /opt/airflow/dbt_venv/bin/activate
    pip install --quiet dbt-postgres
    cd /opt/airflow/my_dwh_project
    dbt run --profiles-dir .
    """

    # 2. Görev: Mevcut sanal ortama gir ve testleri çalıştır
    test_script = """
    source /opt/airflow/dbt_venv/bin/activate
    cd /opt/airflow/my_dwh_project
    dbt test --profiles-dir .
    """

    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=run_script
    )

    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command=test_script
    )

    dbt_run >> dbt_test