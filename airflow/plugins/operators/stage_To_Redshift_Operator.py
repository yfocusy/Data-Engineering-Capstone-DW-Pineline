from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358240'

    copy_sql = """
        COPY {}
        FROM '{}'
        IAM_ROLE 'arn:aws:iam::312996358022:role/yuliudacityredshiftrole'
        FORMAT AS PARQUET
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 provider_context="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.provider_context = provider_context

    def execute(self, context):
        self.log.info('===== StageToRedshiftOperator Starting =====')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        execute_date = context["execution_date"]
        y = execute_date.year
        m = execute_date.month
        d = execute_date.strftime('%Y%m%d')
        self.log.info(f"===== Copy SAS data from S3 to Redshift execution_date = {execute_date} =====")
        s3_path = "s3://{}/{}/year={}/month={}/date={}/".format(self.s3_bucket, self.s3_key, y, m, d)
        self.log.info(f"===== s3_path = {s3_path} =====")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
        )
        redshift.run(formatted_sql)



