# tests/test_rates_loader.py
import requests_mock
from datetime import datetime
from google.cloud import storage
from rates_loader import init_gcs_client, get_gcs_blob, querier, get_rates_on_date, upload_gcs_file
from unittest.mock import patch
import pandas as pd

# Приклад тестових даних
resp_dict = {
    "disclaimer": "Usage subject to terms: https://openexchangerates.org/terms",
    "license": "https://openexchangerates.org/license",
    "timestamp": 1722815999,
    "base": "USD",
    "rates": {
        "AED": 3.673,
        "AFN": 70.428007,
        "ALL": 91.814893
    }
}


# Перевірка чи створюється GCS client
def test_init_gcs_client():
    client = init_gcs_client()
    assert isinstance(client, storage.Client)


# Перевіика коректності отримання blob
def test_get_gcs_blob():
    with patch('google.cloud.storage.Client') as MockClient:
        mock_client = MockClient.return_value
        mock_bucket = mock_client.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        
        bucket_name = 'test_bucket'
        blob_name = 'test_blob'
        
        blob = get_gcs_blob(mock_client, bucket_name, blob_name)
        
        mock_client.bucket.assert_called_with(bucket_name)
        mock_bucket.blob.assert_called_with(blob_name)
        assert blob == mock_blob


# Перевірка функції querier (request -> response)
class TestQuerier:
    # Вдало
    def test_querier_success(self):
        url = 'http://example.com'
        with requests_mock.Mocker() as m:
            m.get(url, json=resp_dict, status_code=200)
            resp = querier(url, 'GET', 'json')
        assert resp == resp_dict

    # Невдало
    def test_querier_failure(self):
        url = 'http://example.com'
        with requests_mock.Mocker() as m:
            m.get(url, status_code=404)
            try:
                querier(url, 'GET', 'json')
            except ValueError as e:
                assert str(e) == f"Request failed. URL: {url}, Request type: GET, Response type: json"


# Перевірка функції get_rates_on_date (отримання pandas df з даними за dt_str)
def test_get_rates_on_date():
    dt_str = '2024-08-01'
    app_id = 'test_app_id'
    base_url = 'http://example.com'

    with patch('rates_loader.querier', return_value=resp_dict) as mock_querier:
        df = get_rates_on_date(dt_str, app_id, base_url)

        expected_data = {
            'dt': [datetime.strptime(dt_str, '%Y-%m-%d').date()] * len(resp_dict['rates']),
            'currency': resp_dict['rates'].keys(),
            'rate': resp_dict['rates'].values()
        }
        expected_df = pd.DataFrame(expected_data)
        
        pd.testing.assert_frame_equal(df, expected_df)


# Перевірка коректності завантаження даних на GCS
def test_upload_gcs_file():
    df = pd.DataFrame({'dt': ['2023-01-01'], 'currency': ['USD'], 'rate': [1.0]})
    
    with patch('google.cloud.storage.Blob') as MockBlob:
        mock_blob = MockBlob.return_value
        mock_blob.exists.return_value = True
        
        result = upload_gcs_file(mock_blob, df, 'parquet')
        
        mock_blob.delete.assert_called_once()
        mock_blob.upload_from_string.assert_called_once()
        assert result == 'SUCCESS'

