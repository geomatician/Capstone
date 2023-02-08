from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        FORMAT AS PARQUET
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(LoadRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        role_arn = 'arn:aws:iam::851835621396:role/myRedshiftRole'
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('Clearing data from Redshift table first')
        redshift.run('DELETE FROM {}'.format(self.table))
        self.log.info('Copying data from S3 to Redshift')

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
       
      
        formatted_sql = LoadRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            role_arn
        )

       
        redshift.run(formatted_sql)
        self.log.info(f"Loaded data into table {self.table}")
