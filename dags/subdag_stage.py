import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.udacity_plugin import (StageToRedshiftOperator)

import sql_create

def stage_table (
        parent_dag_name,
        task_id,
        redshift_conn_id,
        aws_credentials_id,
        s3_bucket,
        s3_events_key,
        s3_log_key,
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
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_events_key,
        file_type = "JSON",
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
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        s3_bucket=s3_bucket,
        s3_key=s3_log_key,
        file_type = "JSON",
    )

    drop_stage_events_table >> create_stage_events_table
    drop_stage_songs_table >> create_stage_songs_table
    create_stage_events_table >> stage_events_to_redshift
    create_stage_songs_table >> stage_songs_to_redshift

    return dag