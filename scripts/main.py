from scripts.rates_loader import init_gcs_client, get_gcs_blob, querier, upload_gcs_file
from scripts.pandas_funcs import create_df_from_dict, add_column
import argparse
import os
from dotenv import load_dotenv
from logging import config, getLogger
from datetime import datetime
import json


with open('scripts/logging.conf') as file:
    conf_file = json.load(file)

config.dictConfig(conf_file)
logger = getLogger()


def load_environment() -> tuple:
    """
    Завантажує із .env файла наступні елементи: BUCKET_NAME, BLOB_FOLDER, APP_ID, BASE_URL

    Returns:
        tuple of elements: BUCKET_NAME, BLOB_FOLDER, APP_ID, BASE_URL
    """

    logger.info('Start loading env')
    load_dotenv()

    BUCKET_NAME = os.getenv('BUCKET_NAME')
    BLOB_FOLDER = os.getenv('BLOB_FOLDER')
    APP_ID = os.getenv('APP_ID')
    BASE_URL = os.getenv('BASE_URL')

    logger.info('Finish loading env')
    return BUCKET_NAME, BLOB_FOLDER, APP_ID, BASE_URL


def main(dt_str):
    """
    Основна функція по завантаженню курсів обміну валют

    Steps:
        1.1. Завантажує env із функції "load_environment"
            1.2. Формує URL для запиту
            1.3. Формує назву BLOB_NAME для збереження в GCS
            1.4. Формує шлях до service key GCS
        2. Створює GCS client (fn: init_gcs_client)
        3. Створює GCS blob (fn: get_gcs_blob)
        4. Відправляє GET запит для отримання курсів валют та дістає колонку 'rates' із курсами (fn: querier) як dict
        5. Формує pandas df із dict із курсами валют та додає колонку із датою (fn: create_df_from_dict, add_column)
        6. Завантажує pandas df на GCS. Якщо блоб за дату існує - видаляє його та завантажує новий (fn: upload_gcs_file)
    
    Args:
        dt_str (str): Дата завантаження
    
    Returns:
        str: Повідомлення success/error
    """

    logger.info('-'*100)
    logger.info('Start process')

    BUCKET_NAME, BLOB_FOLDER, APP_ID, BASE_URL = load_environment()
    logger.info('Env loaded')


    dt = datetime.strptime(dt_str, '%Y-%m-%d').date()
    req_url = f'{BASE_URL}/{dt_str}.json?app_id={APP_ID}'

    BLOB_NAME = f'{BLOB_FOLDER}/rates_{dt_str}.parquet'
    current_dir = os.path.dirname(os.path.abspath(__file__))
    service_key_path = f'{current_dir}/keys/gcp_key.json'

    try:
        client = init_gcs_client(service_key_path)
        blob = get_gcs_blob(client, BUCKET_NAME, BLOB_NAME)
        rates_dict = querier(req_url, 'GET', 'json')['rates']
        df = create_df_from_dict(rates_dict, ['currency', 'rate'], 'index')
        df = add_column(df, 'dt', dt)
        result = upload_gcs_file(blob, df, 'parquet')
        logger.error(f'[{result}] - date {dt_str}.')
        return f'[{result}] - date {dt_str}.'
    except Exception as e:
        logger.error(f'Error: {e}')
        return f'Error: {e}'
    
