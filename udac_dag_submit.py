from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries
import create_tables

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 8, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
           schedule_interval="@hourly",
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_stagingevents_table = PostgresOperator(
    task_id="create_stagingevents_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_SE_TABLE_SQL
)

create_stagingsongs_table = PostgresOperator(
    task_id="create_stagingsongs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.CREATE_SS_TABLE_SQL
)



stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/2018-11-01-events.json",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A/TRAAAAK128F9318786.json",
    json_path="auto"
    #/home/workspace/data/song_data/A/B/C/TRABCEI128F424C983.json
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    DDL=create_tables.CREATE_DIM_users_TABLE_SQL,
    DML=SqlQueries.users_table_insert,
    # Selecting Delete-Load Mode -DL-
    LS='DL',
    dag=dag
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    DDL=create_tables.CREATE_DIM_songs_TABLE_SQL,
    DML=SqlQueries.song_table_insert,
    # Selecting Append-Only Mode -AO-
    LS='AO',
    dag=dag
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    DDL=create_tables.CREATE_DIM_artists_TABLE_SQL,
    DML=SqlQueries.artist_table_insert,
    # Selecting Delete-Load Mode -DL-
    LS='DL',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    DDL=create_tables.CREATE_DIM_time_TABLE_SQL,
    DML=SqlQueries.time_table_insert,
    # Selecting Append-Only Mode -AO-
    LS='AO',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table="songplays",
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_stagingevents_table
start_operator >> create_stagingsongs_table
create_stagingevents_table >> stage_events_to_redshift
create_stagingsongs_table >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_songs_dimension_table
load_songplays_table >> load_artists_dimension_table
load_songplays_table >> load_time_dimension_table
load_songs_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artists_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator