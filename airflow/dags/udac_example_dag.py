from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import LoadRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import DataQualityOperator
from helpers import SqlQueries
from airflow.hooks.S3_hook import S3Hook


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2016, 2, 1),
    'end_date': datetime(2016,3,1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False

}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

s3_bucket_name = "capstonebucketudacity"
    
# Drop and create tables in Redshift
create_table = PostgresOperator(
    task_id="Create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql="sql/create_tables.sql"
)

# Load tables

load_immi_dimension_table = LoadRedshiftOperator(
    task_id='load_immi_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.immigration",
    s3_bucket=s3_bucket_name,
    s3_key="formatted_data/immigration.parquet/",
    provide_context=True
)

load_temp_dimension_table = LoadRedshiftOperator(
    task_id='load_temp_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.temperature",
    s3_bucket=s3_bucket_name,
    s3_key="formatted_data/temperature.parquet/",
    provide_context=True
)

load_codes_dimension_table = LoadRedshiftOperator(
    task_id='load_codes_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="public.airportcode",
    s3_bucket=s3_bucket_name,
    s3_key="formatted_data/codes.parquet/",
    provide_context=True
)


load_fact_table = LoadFactOperator(
    task_id='Load_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table="public.fact",
    append_only=True,
    load_fact_sql = SqlQueries.fact_table_insert
)


run_quality_check_1 = DataQualityOperator(
    task_id='Run_data_quality_check_1',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["public.immigration", "public.temperature", "public.airportcode"]
)

run_quality_check_2 = DataQualityOperator(
    task_id='Run_data_quality_check_2',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["public.fact"]
)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# task dependencies

start_operator >> create_table
create_table >> load_immi_dimension_table
create_table >> load_temp_dimension_table
create_table >> load_codes_dimension_table

load_immi_dimension_table >> load_fact_table
load_temp_dimension_table >> load_fact_table
load_codes_dimension_table >> load_fact_table
load_fact_table >> run_quality_check_1
load_fact_table >> run_quality_check_2
run_quality_check_1 >> end_operator
run_quality_check_2 >> end_operator

