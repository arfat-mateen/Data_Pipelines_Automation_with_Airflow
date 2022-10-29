from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries
from load_dim_table_subdag import load_dim_table_subdag


S3_BUCKET = Variable.get('s3_bucket')
REGION = Variable.get('region')
REDSHIFT_CONN_ID = 'redshift'
DAG_ID = 'Sparkify_dag'

dag_default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(dag_id = DAG_ID,
          default_args = dag_default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id = "create_tables",
    dag = dag,
    postgres_conn_id = REDSHIFT_CONN_ID,
    sql = "create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = REDSHIFT_CONN_ID,
    s3_bucket = S3_BUCKET,
    s3_key = 'log_data',
    region = REGION,
    table = 'staging_events',
    json_fromat = 's3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = REDSHIFT_CONN_ID,
    s3_bucket = S3_BUCKET,
    s3_key = 'song_data',
    region = REGION,
    table = 'staging_songs',
    json_fromat = 'auto'
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    table = 'songplays',
    sql_stmt = SqlQueries.songplay_table_insert,
)

load_user_dim_table_task_id = 'Load_user_dim_table_subdag'
load_user_dimension_table = SubDagOperator(
    subdag = load_dim_table_subdag(
        parent_dag_name = DAG_ID,
        task_id = load_user_dim_table_task_id,
        dag_default_args = dag_default_args,
        redshift_conn_id = REDSHIFT_CONN_ID,
        table = 'users',
        sql_stmt = SqlQueries.user_table_insert,
        truncate_table = True
    ),
    task_id = load_user_dim_table_task_id,
    dag = dag
)

load_song_dim_table_task_id = 'Load_song_dim_table_subdag'
load_song_dimension_table = SubDagOperator(
    subdag = load_dim_table_subdag(
        parent_dag_name = DAG_ID,
        task_id = load_song_dim_table_task_id,
        dag_default_args = dag_default_args,
        redshift_conn_id = REDSHIFT_CONN_ID,
        table = 'songs',
        sql_stmt = SqlQueries.song_table_insert,
        truncate_table = True
    ),
    task_id = load_song_dim_table_task_id,
    dag = dag
)

load_artist_dim_table_task_id = 'Load_artist_dim_table_subdag'
load_artist_dimension_table = SubDagOperator(
    subdag = load_dim_table_subdag(
        parent_dag_name = DAG_ID,
        task_id = load_artist_dim_table_task_id,
        dag_default_args = dag_default_args,
        redshift_conn_id = REDSHIFT_CONN_ID,
        table = 'artists',
        sql_stmt = SqlQueries.artist_table_insert,
        truncate_table = True
    ),
    task_id = load_artist_dim_table_task_id,
    dag = dag
)

load_time_dim_table_task_id = 'Load_time_dim_table_subdag'
load_time_dimension_table = SubDagOperator(
    subdag = load_dim_table_subdag(
        parent_dag_name = DAG_ID,
        task_id = load_time_dim_table_task_id,
        dag_default_args = dag_default_args,
        redshift_conn_id = REDSHIFT_CONN_ID,
        table = 'time',
        sql_stmt = SqlQueries.time_table_insert,
        truncate_table = True
    ),
    task_id = load_time_dim_table_task_id,
    dag = dag
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = 'redshift',
    dq_checks = [
        {'test_query': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'test_query': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift] 
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator