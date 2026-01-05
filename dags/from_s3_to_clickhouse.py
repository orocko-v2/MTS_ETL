from airflow import DAG
import datetime
import pandas
import logging
from pyspark.sql import SparkSession
from airflow.operators.python import PythonOperator
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS operations (
        id UInt64,
        phone String,
        datetime DateTime,
        operation_type String,
        amount Float64
    )
    ENGINE = ReplacingMergeTree
    order by (id);
"""

OWNER = 'orocko'
DAG_ID = 'load_data_to_ch'

args = {
    'owner': OWNER,
    'start_date': datetime.datetime(2025,12,1),
    'catchup': False,
}

dag = DAG(dag_id=DAG_ID, default_args=args, schedule='@daily')

def load_data_from_s3_to_ch(**kwargs):
    spark = SparkSession.builder \
        .appName('S3ToClickhouse') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.530") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    filename = f'{datetime.date.today()}'

    schema = StructType([
    StructField('startDateTime', TimestampType(), False),
    StructField('endDateTime', TimestampType(), False),
    StructField('Usages', ArrayType(StructType([
        StructField('date', TimestampType()),          
        StructField('type', StringType()),             
        StructField('amount', FloatType())             
    ])), True)
])

    df = spark.read.schema(schema).format('json').load(f's3a://phones/{filename}')

    exploded_df = df.withColumn("Usages", explode(col("Usages")))

    df_with_phone = exploded_df.withColumn("phone", regexp_extract(input_file_name(), r'/(\d+)_', 1))

    df_with_id = df_with_phone.withColumn("id", monotonically_increasing_id())

    final_df = df_with_id.select(
        col("id"),
        col("phone"),
        col("Usages.date").alias("datetime"),
        col("Usages.type").alias("operation_type"),
        col("Usages.amount").alias("amount")
    )
    logging.info(final_df.head(5))
    clickhouse_hook = ClickHouseHook(
        clickhouse_conn_id='clickhouse_conn'
    )
    logging.info(final_df.printSchema())
    pandas_df = final_df.toPandas()
    pandas_df['phone'] = pandas_df['phone'].astype('string')
    pandas_df['operation_type'] = pandas_df['operation_type'].astype('string')
    logging.info(pandas_df.info())
    insert_sql = 'INSERT INTO operations VALUES'

    clickhouse_hook.execute(insert_sql, pandas_df.to_dict('records'))
    logging.info('SQL executed')

load_data_from_s3_to_clickhouse = PythonOperator(
    task_id='load_data_from_s3_to_clockhouse',
    python_callable=load_data_from_s3_to_ch,
    dag=dag
)

create_operations_table_ch = ClickHouseOperator(
    task_id='create_operations_table_ch',
    clickhouse_conn_id='clickhouse_conn',
    sql=CREATE_TABLE_QUERY,
    dag=dag
)

trigger_dbt = TriggerDagRunOperator(
    task_id='trigger_dbt',
    trigger_dag_id='dbt',
)

create_operations_table_ch >> load_data_from_s3_to_clickhouse >> trigger_dbt