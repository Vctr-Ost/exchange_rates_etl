Для запуску проекту потрібно:

1. Клонувати репо

   ```git clone https://github.com/Vctr-Ost/skelar_test_task.git .```

3. Для запуску проекту у папку "scripts" потрібно додати наступні обʼєкти:

   2.1. Папка "keys" із файлом "gcp_key.json" у якому знаходиться сервісний ключ до бакету GCS

   2.2. ".env" файл із наступними змінними:

        - "APP_ID" - id застосунку на https://openexchangerates.org/account/app-ids
        
        - BASE_URL - базовий url для API ( BASE_URL=https://openexchangerates.org/api/historical )
        
        - BUCKET_NAME - назва бакету GCS
        
        - BLOB_NAME - назва blob із розширенням файлу ( напр. blob_name.parquet )


4. Далі потрібно створити venv у проекті з джобою:

   ```cd scripts && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt && pip install --upgrade pip && deactivate && cd ..```


5. У папці project збілдити та підняти docker compose:

   ```docker compose up -d```

6. Запустити DAG
