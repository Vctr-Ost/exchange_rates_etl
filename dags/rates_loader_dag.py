import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator


default_args={
    'owner':'airflow',
    'depends_on_past': True,
    'start_date':datetime.datetime(2024, 8, 1),
    'retries': 1,
}

with DAG(
    dag_id="rates_loader",
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=True,
):
    
    start = DummyOperator(task_id='Start')

    bash_script = BashOperator(
        task_id='Bash_Script',
        bash_command='cd /opt/airflow && source scripts/venv/bin/activate && python3 scripts/main.py {{ ds }} && deactivate',
    )

    end = DummyOperator(task_id='End')


    start>>bash_script>>end