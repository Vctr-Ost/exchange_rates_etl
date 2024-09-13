from google.cloud import storage
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from logging import getLogger

logger = getLogger(__name__)


# Створення GCS клієнту
def init_gcs_client(service_key_path: str) -> storage.Client:
    """
    Створює GCS client
    
    Args:
        service_key_path (str): path до сервісного ключа GCS

    Returns:
        storage.Client: клієнт GCS
    """

    logger.info('Start GCS init')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_key_path
    logger.info('Finish GCS init')
    return storage.Client()


# Отримання Blob
def get_gcs_blob(client: storage.Client, bucket_name: str, blob_name: str) -> storage.Blob:
    """
    Отримання blob у бакеті GCS клієнта
    
    Args:
        client (storage.Client): GCS клієнт
        bucket_name (str): назва бакета GCS
        blob_name (str): назва блобу у GCS бакеті

    Returns:
        blob: посилання на блоб
    """

    logger.info('Start GCS blob getting')
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    logger.info('Finish GCS blob getting')
    return blob


# Функція для запитів
def get_querier(url: str):
    """
    Функція для відправляє GET запит по URL

    Args:
        url (str): URL для запиту

    Returns:
        Результат HTTP запиту або помилка (при невдалому запиті)
    """

    logger.info(f'Sending GET query on {url}')
    resp = requests.get(url)
    logger.info(f'Response status code: {resp.status_code}')
    

    if resp.status_code == 200:
        logger.info(f'Query on {url} success')
        return resp
    else:
        logger.error(f"Request failed. URL: {url}")
        raise ValueError(f"Request failed. URL: {url}")


# Upload or Replace df на GCS
def upload_gcs_parquet(blob: storage.Blob, df: pd.DataFrame):  
    """
    Завантажує pandas.DataFrame на GCS у форматі parquet

    Steps:
        1. Виклик функції remove_blob для видалення блобу якщо такий існує
        2. Завантажує pandas.DataFrame на GCS у форматі parquet
    
    Args:
        blob (storage.Blob): посилання на блоб
        df (pd.DataFrame): pandas df який потрібно завантажити

    Returns:
        str: результат завантаження ('SUCCESS' або падає помилка)
    """
    
    try:
        logger.info('Starting uploading blob on GCS')
        blob.upload_from_string(df.to_parquet())
        logger.info('Blob uploaded on GCS')
    except Exception as e:
        raise RuntimeError(f'Помилка при завантаженні файлу: {e}')
