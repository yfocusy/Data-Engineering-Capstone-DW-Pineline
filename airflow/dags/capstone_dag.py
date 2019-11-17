from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from operators.stage_To_Redshift_Operator import StageToRedshiftOperator
from operators.load_Dim_Operator import LoadDimensionOperator
from operators.load_Fact_Operator import LoadFactOperator
from operators.data_quality import DataQualityOperator
from operators.load_Lookups_Operator import LoadLookupOperator

from helpers.sql_queries import SqlQueries
from datetime import datetime, timedelta

dagname = 'capstone9'


default_args = {
    'owner': 'yuliDE',
    'depends_on_past': False,
    'start_date': datetime(2016, 4, 1),
    'email': ['li.yu.unsw@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'catchup':False,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dagname,
    default_args=default_args,
    description='yuli_capstone_project',
    schedule_interval=timedelta(days=10)
)

start_operator = DummyOperator(
    task_id='Start_Execution',
    dag=dag,
)

end_operator = DummyOperator(
    task_id='End_Execution',
    dag=dag,
)


create_tables_operator = PostgresOperator(
    task_id = 'Create_Redshift_Tables',
    postgres_conn_id= 'redshift',
    sql = SqlQueries.create_all_talbes,
    dag = dag,
)

stage_i94immi_operator = StageToRedshiftOperator(
    task_id='Stage_dim_i94immi',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_default',
    table='staging_i94immi',
    s3_bucket='yulicapstone',
    s3_key="i94immi_sas",
    provider_context=True,
    dag = dag,
)

dim_mode_lookup_operator = LoadLookupOperator(
    task_id='Load_dim_mode_lookups',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_default',
    table='dim_mode',
    s3_bucket='yulicapstone',
    s3_key="lookups/1.mode.json",
    ignore_header=0,
    provider_context=True,
    json_path='auto',
    dag = dag,
)


dim_location_lookup_operator = LoadLookupOperator(
    task_id='Load_dim_location_lookups',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_default',
    table='dim_location',
    s3_bucket='yulicapstone',
    s3_key="lookups/2.location.json",
    ignore_header=0,
    provider_context=True,
    json_path='auto',
    dag = dag,
)

dim_port_lookup_operator = LoadLookupOperator(
    task_id='Load_dim_port_lookups',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_default',
    table='dim_port',
    s3_bucket='yulicapstone',
    s3_key="lookups/3.port.json",
    ignore_header=0,
    provider_context=True,
    json_path='auto',
    dag = dag,
)

dim_address_lookup_operator = LoadLookupOperator(
    task_id='Load_dim_address_lookups',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_default',
    table='dim_address',
    s3_bucket='yulicapstone',
    s3_key="lookups/4.address.json",
    ignore_header=0,
    provider_context=True,
    json_path='auto',
    dag = dag,
)

dim_visa_lookup_operator = LoadLookupOperator(
    task_id='Load_dim_visa_lookups',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_default',
    table='dim_visa',
    s3_bucket='yulicapstone',
    s3_key="lookups/5.visa_category.json",
    ignore_header=0,
    provider_context=True,
    json_path='auto',
    dag = dag,
)

dim_airline_lookup_operator = LoadLookupOperator(
    task_id='Load_dim_airline_lookups',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_default',
    table='dim_airline',
    s3_bucket='yulicapstone',
    s3_key="lookups/6.airline.json",
    ignore_header=0,
    provider_context=True,
    json_path='auto',
    dag = dag,
)



load_dim_visitor = LoadDimensionOperator(
    task_id='Load_dim_visitor_table',
    redshift_conn_id='redshift',
    table='dim_visitor',
    sql_list = [SqlQueries.dim_visitor_insert],
    update_mode="overwrite",
    provider_context=True,
    dag = dag,
)


load_dim_time = LoadDimensionOperator(
    task_id='Load_dim_time_table',
    redshift_conn_id='redshift',
    table='dim_time',
    sql_list = [SqlQueries.dim_arrdate_time_insert,SqlQueries.dim_depdate_time_insert],
    update_mode="overwrite",
    provider_context=True,
    dag = dag,
)

load_fact_visit = LoadFactOperator(
    task_id='Load_fact_visit_event_table',
    redshift_conn_id='redshift',
    table='fact_visit_event',
    sql = SqlQueries.fact_visit_insert,
    update_mode="update",
    provider_context=True,
    dag = dag,
)

data_quality_operator = DataQualityOperator(
    task_id='Run_data_quality_tables',
    redshift_conn_id='redshift',
    table_list=['dim_visitor','dim_time','dim_mode','dim_location','dim_port','dim_address','dim_visa','dim_airline','fact_visit_event'],
    provider_context=True,
    dag = dag,
)


dim_list = [dim_mode_lookup_operator,dim_location_lookup_operator,dim_port_lookup_operator,dim_address_lookup_operator, dim_visa_lookup_operator, dim_airline_lookup_operator, load_dim_visitor,load_dim_time]


start_operator >> create_tables_operator >> stage_i94immi_operator >> dim_list >> load_fact_visit >> data_quality_operator >> end_operator