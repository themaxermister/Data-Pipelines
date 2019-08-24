import datetime
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

from subdag_create import create_table
from subdag_stage import stage_table
from subdag_load import init_table
from subdag_check import check_table

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

# STAGE TABLES
stage_task_id = "stage_tables_task"
subdag_stage_task = SubDagOperator(
    subdag=stage_table(
        "dag",
        stage_task_id,
        "redshift",
        "aws_credentials",
        s3_bucket="udacity-dend",
        
        # EVENTS
        s3_events_key="log_data/",
        event_path="s3://udacity-dend/log_json_path.json",

        # SONGS
        s3_songs_key="song_data/A/A/A",
        song_path="auto",

        region = "us-west-2",
        start_date=start_date,
    ),
    task_id=stage_task_id,
    dag=dag,
)

# CREATE TABLES
create_task_id = "create_tables_task"
subdag_create_task = SubDagOperator(
    subdag=create_table(
        "dag",
        create_task_id,
        "redshift",
        start_date=start_date,
    ),
    task_id=create_task_id,
    dag=dag,
)

# LOAD TABLES
load_task_id = "load_tables_task"
subdag_load_task = SubDagOperator(
    subdag=init_table(
        "dag",
        load_task_id,
        "redshift",
        start_date=start_date,
    ),
    task_id=load_task_id,
    dag=dag,
)

# CHECK TABLES
check_task_id = "check_tables_task"
subdag_check_task = SubDagOperator(
    subdag=check_table(
        "dag",
        check_task_id,
        "redshift",
        start_date=start_date,
    ),
    task_id=check_task_id,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> subdag_stage_task
subdag_stage_task >> subdag_create_task
subdag_create_task >> subdag_load_task
subdag_load_task >> subdag_check_task
subdag_check_task >> end_operator