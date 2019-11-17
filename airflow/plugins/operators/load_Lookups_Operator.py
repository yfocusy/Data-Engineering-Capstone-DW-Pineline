from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadLookupOperator(BaseOperator):

    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        FORMAT AS JSON '{}'
    """

    count_sql = """
        SELECT COUNT(*) 
        FROM public.{}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 ignore_header=1,
                 provider_context="",
                 json_path="auto",
                 *args, **kwargs):

        super(LoadLookupOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table=table
        self.s3_bucket =s3_bucket
        self.s3_key=s3_key
        self.ignore_header = ignore_header
        self.provider_context = provider_context
        self.json_path=json_path

    def execute(self, context):
        self.log.info(f'===== LoadLookup dim table = {self.table} =====')
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("===== Copy data mode from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        # s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"===== s3_path = {s3_path} =====")

        count_sql =LoadLookupOperator.count_sql.format(
            self.table
        )

        count = redshift.get_records(count_sql)

        if count[0][0]==0:
            formatted_sql = LoadLookupOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_header,
                self.json_path
            )
            redshift.run(formatted_sql)











