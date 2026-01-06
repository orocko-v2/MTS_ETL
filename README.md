# MTS_ETL
Проект для обработки и анализа данных с использованием современного стека технологий

## Стек технологий

- **Airflow** - оркестрация ETL процессов
- **ClickHouse** - колоночная СУБД
- **Apache Spark** - обработка данных в реальном времени
- **Apache Superset** - визуализация данных
- **DBT** - трансформация данных
- **Docker** - контейнеризация всех компонентов
- **MinIO** - S3-совместимое хранилище объектов
- **PostgreSQL** - метаданные Airflow

## Установка и запуск

1. Клонируйте репозиторий:
```bash
git clone https://github.com/orocko-v2/MTS_ETL.git
cd MTS_ETL
```

2. Создайте файл `.env` со следующими параметрами:
```env
AIRFLOW_UID=50000
AIRFLOW_GID=0
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=admin
CLICKHOUSE_DB=mtsetl

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

DBT_PROFILES_DIR=/opt/airflow/dbt_click

SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin
```

3. Создайте папку `data` со следующими файлами:
   
auth-params.json
```auth-params.json
{
  "login": "MTS_API_login",
  "password": "MTS_API_password"
}
```

phones.csv
```phones.csv
Phones; Name; Other info
```

4. Запустите проект с помощью Docker Compose:
```bash
docker-compose up -d
```

## Доступ
- **Airflow**: http://localhost:8080 
- **Superset**: http://localhost:8088 (admin/admin)
- **MinIO Console**: http://localhost:9001
