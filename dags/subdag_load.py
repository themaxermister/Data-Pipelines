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
        table,
        redshift_conn_id,
        aws_credentials_id,
        drop_sql,
        create_sql,
        s3_bucket,
        s3_key,
        json_path,
        region,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    # STAGE_TABLE
    drop_stage_table = PostgresOperator(
        task_id=f"drop_stage_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=drop_sql,
    )

    create_stage_table = PostgresOperator(
        task_id=f"create_stage_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql,
    )

    stage_to_redshift = StageToRedshiftOperator(
        task_id=f'Stage_{table}',
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        region=region,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        json_path=json_path,
    )

    drop_stage_table >> create_stage_table
    create_stage_table >> stage_to_redshift
    
    return dag