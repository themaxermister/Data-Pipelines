import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import DataQualityOperator

def check_table (
        parent_dag_name,
        task_id,
        redshift_conn_id,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    # SONGPLAYS
    quality_check_songplays  = DataQualityOperator(
        task_id='Run_data_quality_checks_songplays',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table = "public.songplays",
    )

    # ARTISTS
    quality_check_artist  = DataQualityOperator(
        task_id='Run_data_quality_checks_artists',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table = "public.artists",
    )

    # SONGS
    quality_check_song  = DataQualityOperator(
        task_id='Run_data_quality_checks_songs',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table = "public.songs",
    )

    # USERS
    quality_check_users  = DataQualityOperator(
        task_id='Run_data_quality_checks_users',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table = "public.users",
    )

    # TIME
    quality_check_time  = DataQualityOperator(
        task_id='Run_data_quality_check_time',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table = "public.time",
    )

    return dag
