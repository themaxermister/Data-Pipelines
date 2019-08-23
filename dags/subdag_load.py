import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (LoadFactOperator, LoadDimensionOperator)

from helpers.sql_load import SqlQueries

def init_table (
        parent_dag_name,
        task_id,
        redshift_conn_id,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    # SONGPLAYS
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="public.songplays",
        columns="start_time, user_id, level, song_id, artist_id, session_id, location, user_agent",
        insert_query=SqlQueries.songplay_table_insert,

    )

    # ARTISTS
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="public.artists",
        columns="artist_id, name, location, latitude, longitude",
        insert_query=SqlQueries.artist_table_insert,
    )

    # SONGS
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_songs_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="public.songs",
        columns="song_id, title, artist_id, year, duration",
        insert_query=SqlQueries.song_table_insert,
    )

    # USERS
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="public.users",
        columns="user_id, first_name, last_name, gender, level",
        insert_query=SqlQueries.user_table_insert,
    )

    # TIME
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="public.time",
        columns="start_time, hour, day, week, month, year, weekday",
        insert_query=SqlQueries.time_table_insert,
    )

    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_time_dimension_table

    return dag