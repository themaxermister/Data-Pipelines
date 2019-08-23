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
        FORMAT AS {} '{}'
        REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_type = "",
                 region = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Staging {}.format"(self.table))
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.file_type = file_type
            self.file_path = file_path
        )
        redshift.run(formatted_sql)
        self.log.info("{} STAGED".format(self.table))

# The stage operator is expected to be able to load any JSON formatted
# files from 53 to Amazon Redshift. The operator creates and runs a SQL
# COPY statement based on the parameters provided. The operator's parameters 
# should specify where in S3 the file is loaded and what is the target table. 

# The parameters should be used to distinguish between JSON file.
# Another important requirement of the stage operator is containing
# a templated field that allows it to load timestamped files from 53 
# based on the execution time and run backfills. 


