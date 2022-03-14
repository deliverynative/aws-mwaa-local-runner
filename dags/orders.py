from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


dag = DAG("Orders", description='Read PG DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

get_orders = SqlToS3Operator(
    task_id="get_orders",
    query="SELECT * FROM orders WHERE updated_at >= %(begin_date)s",
    s3_bucket="deliverynative-etl-data",
    s3_key="orders",
    replace="true",
    sql_conn_id="postgres_default",
    parameters={"begin_date": "2022-02-01"},
    aws_conn_id="aws_default",
                dag=dag
)


get_orders
