import logging

from airflow import DAG
from airflow.operators import LoadDimensionOperator

def load_dim_table_subdag(
        parent_dag_name,
        task_id,
        dag_default_args,
        redshift_conn_id,
        table,
        sql_stmt,
        truncate_table,
        *args, **kwargs):

    dag = DAG(dag_id = f'{parent_dag_name}.{task_id}', 
              default_args = dag_default_args, 
              **kwargs)

    load_dim_table = LoadDimensionOperator(
            task_id = f'Load_{table}_dim_table',
            dag = dag,
            redshift_conn_id = redshift_conn_id,
            sql_stmt = sql_stmt,
            table = table,
            truncate_table = truncate_table
    )

    return dag
