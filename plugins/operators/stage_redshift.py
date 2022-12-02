from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 region,
                 json_option,
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers,
        self.region=region,
        self.json_option = json_option
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(self.redshift_conn_id)
        
        rendered_key = self.s3_key.format(**context)
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"s3_path: {s3_path}")
        self.log.info(f"Staging data staging from bucket {self.s3_bucket} (S3) to table {self.table} (Redshift)")
        
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            #self.ignore_headers,
            #self.delimiter,
            self.region,
            self.json_option
        )
        
        redshift.run(formatted_sql)
        self.log.info(f"Finished data staging from bucket {self.s3_bucket} (S3) to table {self.table} (Redshift)")




