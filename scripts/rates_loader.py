from google.cloud import storage
from datetime import datetime
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from logging import getLogger

logger = getLogger(__name__)


# Створення GCS клієнту
def init_gcs_client(service_key_path: str) -> storage.Client:
    logger.info('Start GCS init')
    load_dotenv()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_key_path
    logger.info('Finish GCS init')
    return storage.Client()


# Отримання Blob
def get_gcs_blob(client: storage.Client, bucket_name: str, blob_name: str) -> storage.Blob:
    logger.info('Start GCS blob getting')
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    logger.info('Finish GCS blob getting')
    return blob


# Функція для запитів
def querier(url: str, req_type: str, resp_type: str) -> dict:
    if req_type == 'GET':
        logger.info(f'Sending GET query on {url}')
        resp = requests.get(url)
        logger.info(f'Response status code: {resp.status_code}')
    else:
        logger.error(f'Unsupported request type. URL: {url}, Request type: {req_type}')
        raise ValueError(f'Unsupported request type. URL: {url}, Request type: {req_type}')

    if resp.status_code == 200:
        if resp_type == 'json':
            logger.info(f'Query on {url} success')
            return resp.json()
    else:
        logger.error(f"Request failed. URL: {url}, Request type: {req_type}, Response type: {resp_type}")
        raise ValueError(f"Request failed. URL: {url}, Request type: {req_type}, Response type: {resp_type}")


# Функція створює pandas df із курсами валют по конкретній даті
def get_rates_on_date(dt_str: str, APP_ID: str, BASE_URL: str) -> pd.DataFrame:
    dt = datetime.strptime(dt_str, '%Y-%m-%d').date()
    req_url = f'{BASE_URL}/{dt}.json?app_id={APP_ID}'
    logger.info(f'Query to {req_url}, date: {dt_str}')
    
    rates_dict = querier(req_url, 'GET', 'json')['rates']

    # convert rates_dict to pandas df and add column with date
    df = pd.DataFrame.from_dict(rates_dict, orient='index', columns=['rate']).reset_index()
    logger.info('Df created')
    df.columns = ['currency', 'rate']
    df['dt'] = dt
    logger.info('Dt column added')
    df = df[['dt', 'currency', 'rate']]
    logger.info('Df columns sorted')

    return df


# Видаляє blob із GCS якщо такий існує
def remove_blob(blob: storage.Blob):
    if blob.exists():
        logger.info('Starting removing blob from GCS')
        blob.delete()
        logger.info('Blob removed from GCS')


# Upload or Replace df на GCS
def upload_gcs_file(blob: storage.Blob, df: pd.DataFrame, file_type: str) -> str:  
    remove_blob(blob)

    if file_type == 'parquet':
        logger.info('Starting uploading blob on GCS')
        blob.upload_from_string(df.to_parquet(), if_generation_match=0)
        logger.info('Blob uploaded on GCS')
        return 'SUCCESS'
    else:
        logger.error(f"Unsupported file type for: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")
