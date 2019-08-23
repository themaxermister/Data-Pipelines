import datetime
import logging
import sql

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import sql_create

def create_table (
        parent_dag_name,
        redshift_conn_id,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
)

# SONGSPLAY_TABLE [FACT]
drop_songplays_table = PostgresOperator(
    task_id="drop_songplays_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.DROP_SONGPLAYS_TABLE
)

create_songplays_table = PostgresOperator(
    task_id="create_songplays_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.CREATE_SONGPLAYS_TABLE
)

# ARTISTS_TABLE [DIMENSIONAL]
drop_artists_table = PostgresOperator(
    task_id="drop_artists_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.DROP_ARTISTS_TABLE
)

create_artists_table = PostgresOperator(
    task_id="create_artists_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.CREATE_ARTISTS_TABLE
)

# SONGS_TABLE [DIMENSIONAL]
drop_songs_table = PostgresOperator(
    task_id="drop_songs_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.DROP_SONGS_TABLE
)

create_songs_table = PostgresOperator(
    task_id="create_songs_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.CREATE_SONGS_TABLE
)

# USERS_TABLE [DIMENSIONAL]
drop_users_table = PostgresOperator(
    task_id="drop_users_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.DROP_USERS_TABLE
)

create_users_table = PostgresOperator(
    task_id="create_users_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.CREATE_USERS_TABLE
)

# TIME_TABLE [DIMENSIONAL]
drop_time_table = PostgresOperator(
    task_id="drop_time_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.DROP_TIME_TABLE
)

create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id=redshift_conn_id,
    sql=sql_create.CREATE_TIME_TABLE
)

drop_songplays_table >> create_songplays_table
drop_artists_table >> create_artists_table
drop_songs_table >> create_songs_table
drop_users_table >> create_users_table
drop_time_table >> create_time_table