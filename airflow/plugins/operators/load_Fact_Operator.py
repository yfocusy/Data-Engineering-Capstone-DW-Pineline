from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",
                 update_mode="",
                 provider_context="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.update_mode = update_mode
        self.provider_context = provider_context

    def execute(self, context):
        self.log.info('===== LoadFactOperator Staring =====')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('===== Load Fact table {} ====='.format(self.table))
        execute_date = context["execution_date"]
        y = execute_date.year
        m = execute_date.month
        d = execute_date.strftime('%Y%m%d')

        self.log.info(type(d))
        self.log.info(d)
        columns = "visitor_id, cit_id, res_id,port_code,address_code,mode_id,visa_id,visatype,fltno,airline_iata,arrdate_id,depdate_id"

        # insert_query = 'INSERT INTO {} ({} WHERE arrdate={})'.format(self.table, self.sql, d)
        insert_query = 'INSERT INTO {} ({}) {} WHERE arrdate={}'.format(self.table, columns, self.sql, d)

        redshift.run(insert_query)
