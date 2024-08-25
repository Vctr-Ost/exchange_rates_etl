Для запуску проекту потрібно:

1. Клонувати репо

   ```git clone https://github.com/Vctr-Ost/skelar_test_task.git .```

3. Для запуску проекту у папку "scripts" потрібно додати наступні обʼєкти:

   2.1. Папка "keys" із файлом "gcp_key.json" у якому знаходиться сервісний ключ до бакету GCS

   2.2. ".env" файл із наступними змінними:

        - "APP_ID" - id застосунку на openexchangerates.org
        
        - BASE_URL - базовий url для API на openexchangerates.org
        
        - BUCKET_NAME - назва бакету GCS
        
        - BLOB_FOLDER - назва папки для блобів у бакеті

4. У папці проекту потрібно підняти docker compose:

   ```docker compose up -d```

5. Запустити DAG на localhost:8080
