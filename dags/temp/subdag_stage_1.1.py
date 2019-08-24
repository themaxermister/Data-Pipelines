import datetime
import sql
import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import (StageToRedshiftOperator)

import sql_create

def stage_table (
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        region,
        s3_bucket,

        # LOG DATA
        s3_events_key,
        event_path,

        # SONG DATA
        s3_songs_key,
        song_path,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    # STAGE_EVENTS_TABLE
    drop_stage_events_table = PostgresOperator(
        task_id="drop_stage_events_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=sql_create.DROP_STAGE_EVENTS_TABLE
    )

    create_stage_events_table = PostgresOperator(
        task_id="create_stage_events_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=sql_create.CREATE_STAGE_EVENTS_TABLE
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        table="public.staging_events",
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        region=region,
        s3_bucket=s3_bucket,
        s3_key=s3_events_key,
        json_path=event_path
    )


    # STAGE_SONGS_TABLE
    drop_stage_songs_table = PostgresOperator(
        task_id="drop_stage_songs_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=sql_create.DROP_STAGE_SONGS_TABLE
    )

    create_stage_songs_table = PostgresOperator(
        task_id="create_stage_songs_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=sql_create.CREATE_STAGE_SONGS_TABLE
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        table="public.staging_songs",
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        region=region,
        s3_bucket=s3_bucket,
        s3_key=s3_songs_key,
        json_path=song_path
    )

    drop_stage_events_table >> create_stage_events_table
    drop_stage_songs_table >> create_stage_songs_table
    create_stage_events_table >> stage_events_to_redshift
    create_stage_songs_table >> stage_songs_to_redshift

    return dag