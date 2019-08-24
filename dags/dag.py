from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from subdag_create import create_table
from subdag_stage import stage_table
from subdag_load import init_table
from subdag_check import check_table

import sql_create
from helpers import SqlQueries

start_date = datetime.datetime.utcnow()

default_args = {
    'owner': 'Max',
    'start_date': start_date
    #'schedule_interval': "@hourly",
    #'retries': 3,
    #'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG('dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# STAGE EVENT TABLE
stage_event_task_id = "stage_event_table_task"
subdag_stage_event_task = SubDagOperator(
    subdag=stage_table(
        "dag",
        stage_event_task_id,
        "public.staging_events",
        "redshift",
        "aws_credentials",
        drop_sql=sql_create.DROP_STAGE_EVENTS_TABLE,
        create_sql=sql_create.CREATE_STAGE_EVENTS_TABLE,
        s3_bucket="udacity-dend",
        s3_key="log_data/",
        json_path="s3://udacity-dend/log_json_path.json",
        region = "us-west-2",
        start_date=start_date,
    ),
    task_id=stage_event_task_id,
    dag=dag,
)

# STAGE SONG TABLE
stage_song_task_id = "stage_event_table_task"
subdag_stage_song_task = SubDagOperator(
    subdag=stage_table(
        "dag",
        stage_song_task_id,
        "public.staging_songs",
        "redshift",
        "aws_credentials",
        drop_sql=sql_create.DROP_STAGE_SONGS_TABLE,
        create_sql=sql_create.CREATE_STAGE_SONGS_TABLE,
        s3_bucket="udacity-dend",
        s3_key="song_data/A/A/A",
        json_path="auto",
        region = "us-west-2",
        start_date=start_date,
    ),
    task_id=stage_song_task_id,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_end_operator
