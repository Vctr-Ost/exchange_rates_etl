from rates_loader import init_gcs_client, get_gcs_blob, get_rates_on_date, upload_gcs_file
import argparse
import os
from dotenv import load_dotenv
from logging import config, getLogger
import json


with open('logging.conf') as file:
    conf_file = json.load(file)

config.dictConfig(conf_file)
logger = getLogger()


def load_environment() -> tuple:
    logger.info('Start loading env')
    load_dotenv()
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    BLOB_FOLDER = os.getenv('BLOB_FOLDER')
    APP_ID = os.getenv('APP_ID')
    BASE_URL = os.getenv('BASE_URL')
    logger.info('Finish loading env')
    return BUCKET_NAME, BLOB_FOLDER, APP_ID, BASE_URL


def get_cli_args() -> argparse.Namespace:
    logger.info('Start loading CLI args')
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str, help="Format: 'yyyy-mm-dd'")
    logger.info('Finish loading CLI args')
    return parser.parse_args()


def main():
    logger.info('-'*100)
    logger.info('Start process')

    BUCKET_NAME, BLOB_FOLDER, APP_ID, BASE_URL = load_environment()
    logger.info('Env loaded')

    dt = get_cli_args().date
    logger.info('CLI args loaded')

    BLOB_NAME = f'{BLOB_FOLDER}/rates_{dt}.parquet'
    current_dir = os.path.dirname(os.path.abspath(__file__))
    service_key_path = f'{current_dir}/keys/gcp_key.json'

    try:
        client = init_gcs_client(service_key_path)
        blob = get_gcs_blob(client, BUCKET_NAME, BLOB_NAME)
        df = get_rates_on_date(dt, APP_ID, BASE_URL)
        result = upload_gcs_file(blob, df, 'parquet')
        print(f'[{result}] - date {dt}.')
    except Exception as e:
        print(f'[ERROR] - {e}')
    
    logger.info(f'Finish process, date: {dt}')


if __name__ == '__main__':
    main()
