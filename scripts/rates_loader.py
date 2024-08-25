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
def querier(url: str, req_type: str, resp_type: str):
    """
    Функція для запитів

    Steps:
        1. Функція дивиться на тип запиту (req_type) та відправляє request із потрібним типом на URL
        2. Якщо status_code відповіді == 200 - повертаємо відповідь у потрібному форматі (resp_type)
    
    Args:
        url (str): URL для запиту
        req_type (str): тип запиту (GET/POST/UPDATE/...)
        resp_type (str): формат повернення відповіді (напр. json)

    Returns:
        Результат HTTP запиту або помилка (при невдалому запиті)
    """

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


# Видаляє blob із GCS якщо такий існує
def remove_blob(blob: storage.Blob):
    """
    Перевіряє чи є blob у GCS та видаляє його якщо він є
    
    Args:
        blob (storage.Blob): посилання на блоб

    Returns:
        None
    """

    if blob.exists():
        logger.info('Starting removing blob from GCS')
        blob.delete()
        logger.info('Blob removed from GCS')


# Upload or Replace df на GCS
def upload_gcs_file(blob: storage.Blob, df: pd.DataFrame, file_type: str) -> str:  
    """
    Завантажує pandas.DataFrame на GCS у потрібному форматі

    Steps:
        1. Виклик функції remove_blob для видалення блобу якщо такий існує
        2. Взалежності від file_type - завантажує на GCS pandas.DataFrame у потрібному форматі
    
    Args:
        blob (storage.Blob): посилання на блоб
        df (pd.DataFrame): pandas df який потрібно завантажити
        file_type (str): тип файлу завантаження (наразі тільки parquet)

    Returns:
        str: результат завантаження ('SUCCESS' або падає помилка)
    """
    
    remove_blob(blob)

    if file_type == 'parquet':
        logger.info('Starting uploading blob on GCS')
        blob.upload_from_string(df.to_parquet(), if_generation_match=0)
        logger.info('Blob uploaded on GCS')
        return 'SUCCESS'
    else:
        logger.error(f"Unsupported file type for: {file_type}")
        raise ValueError(f"Unsupported file type: {file_type}")
