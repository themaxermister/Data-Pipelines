import datetime
import sql
import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.udacity_plugin import (LoadFactOperator, LoadDimensionOperator)

'''
Subdag that drops, create and loads a table from S3
'''

def init_table (
        parent_dag_name,
        task_id,
        table,
        redshift_conn_id,
        drop_sql,
        create_sql,
        load_sql,
        fact = False,
        *args, **kwargs):
   
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    # DROP
    drop_table = PostgresOperator(
        task_id=f"drop_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=drop_sql,
    )

    # CREATE
    create_table = PostgresOperator(
        task_id=f"create_{table}_table",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql,
    )

    if fact:
        # LOAD
        load_table = LoadFactOperator(
            task_id=f'Load_{table}_fact_table',
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            table=table,
            insert_query=load_sql,

        )
    else:
        load_table = LoadDimensionOperator(
            task_id=f'Load_{table}_dim_table',
            dag=dag,
            redshift_conn_id=redshift_conn_id,
            table=table,
            insert_query=load_sql,
        )   

    drop_table >> create_table
    create_table >> load_table
   
    return dag