from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_list="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list

    def execute(self, context):
        self.log.info('===== DataQualityOperator Staring =====')

        # table_pks = {'dim_visitor':'visitor_id','dim_time':'time_id','dim_mode':'mode_id','dim_location':'location_id','dim_port':'port_code','dim_address':'address_code','dim_visa':'visa_id','dim_airline':'airline_id','fact_visit_event':'event_id'}


        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.table_list:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"===== Data quality check failed. {table} has no values")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"===== Data quality check failed. {table} contained 0 rows")
            self.log.info(f"===== Data quality on table {table} check passed with {records[0][0]} records")


            if table =='fact_visit_event':
                count = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}, dim_visitor WHERE {table}.event_id = records and dim_visitor.visitor_id = {table}.visitor_id")
                if count !=1:
                    raise ValueError(f"===== Data quality check failed. {table} has not match the vistor constraint = {count}")
                self.log.info(f"===== Data quality on table {table} check passed with the visitor_id record")
