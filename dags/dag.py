import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

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

# STAGE EVENT TABLE
stage_song_task_id = "stage_song_table_task"
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

load_songplays_task_id = "load_songplays_tables_task"
subdag_load_songplays_task = SubDagOperator(
    subdag=init_table(
        "dag",
        load_songplays_task_id,
        "public.songplays",
        "redshift",
        drop_sql= sql_create.DROP_SONGPLAYS_TABLE,
        create_sql= sql_create.CREATE_SONGPLAYS_TABLE,
        load_sql= SqlQueries.songplay_table_insert,
        fact= True,
        start_date=start_date,
    ),
    task_id=load_songplays_task_id,
    dag=dag,
)

# LOAD SONGS TABLE
load_songs_task_id = "load_songs_tables_task"
subdag_load_songs_task = SubDagOperator(
    subdag=init_table(
        "dag",
        load_songs_task_id,
        "public.songs",
        "redshift",
        drop_sql= sql_create.DROP_SONGS_TABLE,
        create_sql= sql_create.CREATE_SONGS_TABLE,
        load_sql= SqlQueries.song_table_insert,
        start_date=start_date,
    ),
    task_id=load_songs_task_id,
    dag=dag,
)

# LOAD USERS TABLE
load_users_task_id = "load_users_tables_task"
subdag_load_users_task = SubDagOperator(
    subdag=init_table(
        "dag",
        load_users_task_id,
        "public.users",
        "redshift",
        drop_sql= sql_create.DROP_USERS_TABLE,
        create_sql= sql_create.CREATE_USERS_TABLE,
        load_sql= SqlQueries.user_table_insert,
        start_date=start_date,
    ),
    task_id=load_users_task_id,
    dag=dag,
)

# LOAD SONGS TABLE
load_songs_task_id = "load_songs_tables_task"
subdag_load_songs_task = SubDagOperator(
    subdag=init_table(
        "dag",
        load_songs_task_id,
        "public.songs",
        "redshift",
        drop_sql= sql_create.DROP_SONGS_TABLE,
        create_sql= sql_create.CREATE_SONGS_TABLE,
        load_sql= SqlQueries.song_table_insert,
        start_date=start_date,
    ),
    task_id=load_songs_task_id,
    dag=dag,
)

# LOAD ARTISTS TABLE
load_artists_task_id = "load_artists_tables_task"
subdag_load_artists_task = SubDagOperator(
    subdag=init_table(
        "dag",
        load_artists_task_id,
        "public.artists",
        "redshift",
        drop_sql= sql_create.DROP_ARTISTS_TABLE,
        create_sql= sql_create.CREATE_ARTISTS_TABLE,
        load_sql= SqlQueries.artist_table_insert,
        start_date=start_date,
    ),
    task_id=load_artists_task_id,
    dag=dag,
)

# LOAD USERS TABLE
load_users_task_id = "load_users_tables_task"
subdag_load_users_task = SubDagOperator(
    subdag=init_table(
        "dag",
        load_users_task_id,
        "public.users",
        "redshift",
        drop_sql= sql_create.DROP_USERS_TABLE,
        create_sql= sql_create.CREATE_USERS_TABLE,
        load_sql= SqlQueries.user_table_insert,
        start_date=start_date,
    ),
    task_id=load_users_task_id,
    dag=dag,
)

# LOAD TIME TABLE
load_time_task_id = "load_time_tables_task"
subdag_load_time_task = SubDagOperator(
    subdag=init_table(
        "dag",
        load_time_task_id,
        "public.time",
        "redshift",
        drop_sql= sql_create.DROP_TIME_TABLE,
        create_sql= sql_create.CREATE_TIME_TABLE,
        load_sql= SqlQueries.time_table_insert,
        start_date=start_date,
    ),
    task_id=load_time_task_id,
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

start_operator >> subdag_stage_event_task
start_operator >> subdag_stage_song_task
subdag_stage_event_task >> subdag_load_songplays_task
subdag_stage_song_task >> subdag_load_songplays_task
subdag_load_songplays_task >> subdag_load_songs_task
subdag_load_songplays_task >> subdag_load_artists_task
subdag_load_songplays_task >> subdag_load_users_task
subdag_load_songplays_task >> subdag_load_time_task
subdag_load_songs_task >> subdag_check_task
subdag_load_artists_task >> subdag_check_task
subdag_load_users_task >> subdag_check_task
subdag_load_time_task >> subdag_check_task
subdag_check_task >> end_operator