from scripts.rates_loader import init_gcs_client, get_gcs_blob, get_querier, upload_gcs_parquet
from scripts.pandas_funcs import create_df_from_dict, add_column
import argparse
import os
from dotenv import load_dotenv
from logging import config, getLogger
from datetime import datetime
import json
from google.cloud import secretmanager


with open('scripts/logging.conf') as file:
    conf_file = json.load(file)

config.dictConfig(conf_file)
logger = getLogger()


def get_secret(service_key_path, secret_id, version_id="latest"):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_key_path

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/994429214886/secrets/{secret_id}/versions/{version_id}"

    response = client.access_secret_version(name=name)

    return response.payload.data.decode('UTF-8')



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

    current_dir = os.path.dirname(os.path.abspath(__file__))
    service_key_path = f'{current_dir}/keys/gcp_key.json'

    BASE_URL='https://openexchangerates.org/api/historical'
    BUCKET_NAME='skelar_08_2024'
    BLOB_FOLDER='exchange_rates'
    APP_ID = get_secret(service_key_path, 'openexchangerates_app_id')
    
    logger.info('Env loaded')


    dt = datetime.strptime(dt_str, '%Y-%m-%d').date()
    req_url = f'{BASE_URL}/{dt_str}.json?app_id={APP_ID}'

    BLOB_NAME = f'{BLOB_FOLDER}/rates_{dt_str}.parquet'
    

    try:
        client = init_gcs_client(service_key_path)
        blob = get_gcs_blob(client, BUCKET_NAME, BLOB_NAME)
        rates_resp = get_querier(req_url)
        rates_dict = rates_resp.json()['rates']

        df = create_df_from_dict(rates_dict, ['currency', 'rate'], 'index')
        df = add_column(df, 'dt', dt)
        upload_gcs_parquet(blob, df)
        logger.info(f'[SUCCESS] - date {dt_str}.')
    except Exception as e:
        logger.error(f'Error: {e}')
        return f'Error: {e}'
    
