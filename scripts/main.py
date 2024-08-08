from rates_loader import init_gcs_client, get_gcs_blob, get_rates_on_date, upload_gcs_file
import argparse
import os
from dotenv import load_dotenv


def get_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str, help="Format: 'yyyy-mm-dd'")
    return parser.parse_args()


def load_environment() -> tuple:
    load_dotenv()
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    BLOB_FOLDER = os.getenv('BLOB_FOLDER')
    APP_ID = os.getenv('APP_ID')
    BASE_URL = os.getenv('BASE_URL')
    return BUCKET_NAME, BLOB_FOLDER, APP_ID, BASE_URL


def main():
    BUCKET_NAME, BLOB_FOLDER, APP_ID, BASE_URL = load_environment()
    dt = get_cli_args().date
    BLOB_NAME = f'{BLOB_FOLDER}/rates_{dt}.parquet'

    try:
        client = init_gcs_client()
        blob = get_gcs_blob(client, BUCKET_NAME, BLOB_NAME)
        df = get_rates_on_date(dt, APP_ID, BASE_URL)
        result = upload_gcs_file(blob, df, 'parquet')
        print(f'[{result}] - date {dt}.')
    except Exception as e:
        print(f'[ERROR] - {e}')


if __name__ == '__main__':
    main()
