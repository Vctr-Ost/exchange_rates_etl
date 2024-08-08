from google.cloud import storage
from datetime import datetime
import pandas as pd
import requests
import os
from dotenv import load_dotenv


# Створення GCS клієнту
def init_gcs_client() -> storage.Client:
    load_dotenv()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f'{current_dir}/keys/gcp_key.json'
    return storage.Client()

# Отримання Blob
def get_gcs_blob(client: storage.Client, bucket_name: str, blob_name: str) -> storage.Blob:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob


# Функція для запитів
def querier(url: str, req_type: str, resp_type: str) -> dict:
    if req_type == 'GET':
        resp = requests.get(url)
    else:
        raise ValueError(f'Unsupported request type. URL: {url}, Request type: {req_type}')

    if resp.status_code == 200:
        if resp_type == 'json':
            return resp.json()
    else:
        raise ValueError(f"Request failed. URL: {url}, Request type: {req_type}, Response type: {resp_type}")


# Функція створює pandas df із курсами валют по конкретній даті
def get_rates_on_date(dt_str: str, APP_ID: str, BASE_URL: str) -> pd.DataFrame:
    dt = datetime.strptime(dt_str, '%Y-%m-%d').date()
    req_url = f'{BASE_URL}/{dt}.json?app_id={APP_ID}'
    rates_dict = querier(req_url, 'GET', 'json')['rates']

    # convert rates_dict to pandas df and add column with date
    df = pd.DataFrame.from_dict(rates_dict, orient='index', columns=['rate']).reset_index()
    df.columns = ['currency', 'rate']
    df['dt'] = dt
    df = df[['dt', 'currency', 'rate']]

    return df


# Видаляє blob із GCS якщо такий існує
def remove_blob(blob: storage.Blob):
    if blob.exists():
        blob.delete()


# Upload or Replace df на GCS
def upload_gcs_file(blob: storage.Blob, df: pd.DataFrame, file_type: str) -> str:  
    remove_blob(blob)

    if file_type == 'parquet':
        blob.upload_from_string(df.to_parquet(), if_generation_match=0)
        return 'SUCCESS'
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
