from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow')

from ingestion.football_data_org_api.fixtures import main as ingest_fixtures
from ingestion.football_data_org_api.standings import main as ingest_standings
from ingestion.football_data_org_api.top_scorers import main as ingest_top_scorers

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_football_ingestion',
    default_args=default_args,
    description='Daily ingestion of football data from football-data.org API',
    schedule_interval='0 7 * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['football', 'ingestion'],
) as dag:

    ingest_fixtures_task = PythonOperator(
        task_id='ingest_fixtures',
        python_callable=ingest_fixtures,
    )

    ingest_standings_task = PythonOperator(
        task_id='ingest_standings',
        python_callable=ingest_standings,
    )

    ingest_top_scorers_task = PythonOperator(
        task_id='ingest_top_scorers',
        python_callable=ingest_top_scorers,
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow && dbt run --profiles-dir /opt/airflow/dbt',
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow && dbt test --profiles-dir /opt/airflow/dbt',
    )

    [ingest_fixtures_task, ingest_standings_task, ingest_top_scorers_task] >> dbt_run_task >> dbt_test_task