from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_list=[],
                 update_mode="",
                 provider_context="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_list = sql_list
        self.update_mode = update_mode
        self.provider_context = provider_context

    def execute(self, context):
        self.log.info('===== LoadDimensionOperator Staring =====')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('===== Load dimension table {}'.format(self.table))
        execute_date = context["execution_date"]
        y = execute_date.year
        m = execute_date.month
        d = execute_date.strftime('%Y%m%d')

        if len(self.sql_list)!=0:
            for sql in self.sql_list:
                if self.update_mode == 'overwrite':
                    update_query = 'TRUNCATE {}; INSERT INTO {} ({} WHERE arrdate={})'.format(self.table, self.table, sql, d)
                elif self.update_mode == 'insert':
                    update_query = 'INSERT INTO {} ({} WHERE arrdate={})'.format(self.table, sql, d)
                redshift.run(update_query)

