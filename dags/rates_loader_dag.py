from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import logging


def load_data(**kwargs):
    logging.info('Airflow - loading exchange rates function started')

    from scripts.main import main
    exec_date = kwargs['ds']
    tomorow_date = datetime.strptime(exec_date, '%Y-%m-%d') + timedelta(days=1)

    logging.info(f'Airflow - loading exchange rates on date {tomorow_date.strftime('%Y-%m-%d')}')

    main(tomorow_date.strftime('%Y-%m-%d'))
    
    logging.info('Airflow - loading exchange rates function finished')


default_args={
    'owner':'airflow',
    'depends_on_past': True,
    'start_date':datetime(2024, 9, 7),
    'retries': 1,
}


with DAG(
    dag_id="rates_loader_cli",
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=True,
):
    
    start = DummyOperator(task_id='Start')

    python_fn = PythonOperator(
        task_id='python_fn',
        python_callable=load_data,
        provide_context=True,
    )

    end = DummyOperator(task_id='End')


    start>>python_fn>>end

