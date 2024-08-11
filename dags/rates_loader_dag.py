import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonVirtualenvOperator


default_args={
    'owner':'airflow',
    'depends_on_past': True,
    'start_date':datetime.datetime(2024, 7, 1),
    'retries': 1,
}


with DAG(
    dag_id="rates_loader_cli",
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=True,
):
    
    start = DummyOperator(task_id='Start')

    bash_script = BashOperator(
        task_id='Bash_Script',
        bash_command='cd /opt/airflow/scripts && source venv/bin/activate && python3 main.py {{ ds }} && deactivate && cd ..',
    )

    end = DummyOperator(task_id='End')


    start>>bash_script>>end
