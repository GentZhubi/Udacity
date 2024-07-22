from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

default_args = {
    'owner': 'gent-udacity',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():

    @task
    def start_execution():
        return DummyOperator(task_id='Begin_execution')

    @task
    def stage_events():
        return StageToRedshiftOperator(
            task_id='Stage_events',
            redshift_conn_id='redshift',
            aws_credentials_id='aws_credentials',
            table='staging_events',
            s3_bucket='sparkify-lake-house',
            s3_key='log_data/{{ execution_date.year }}/{{ execution_date.month }}/{{ ds }}-events.json',
            json_path='s3://sparkify-lake-house/log_json_path.json'
        )

    @task
    def stage_songs():
        return StageToRedshiftOperator(
            task_id='Stage_songs',
            redshift_conn_id='redshift',
            aws_credentials_id='aws_credentials',
            table='staging_songs',
            s3_bucket='sparkify-lake-house',
            s3_key='song_data',
            json_path='auto'
        )

    @task
    def load_songplays():
        return LoadFactOperator(
            task_id='Load_songplays_fact_table',
            redshift_conn_id='redshift',
            table='songplays',
            sql_query=SqlQueries.songplay_table_insert
        )

    @task
    def load_user_dimension():
        return LoadDimensionOperator(
            task_id='Load_user_dim_table',
            redshift_conn_id='redshift',
            table='users',
            sql_query=SqlQueries.user_table_insert,
            truncate_table=True
        )

    @task
    def load_song_dimension():
        return LoadDimensionOperator(
            task_id='Load_song_dim_table',
            redshift_conn_id='redshift',
            table='songs',
            sql_query=SqlQueries.song_table_insert,
            truncate_table=True
        )

    @task
    def load_artist_dimension():
        return LoadDimensionOperator(
            task_id='Load_artist_dim_table',
            redshift_conn_id='redshift',
            table='artists',
            sql_query=SqlQueries.artist_table_insert,
            truncate_table=True
        )

    @task
    def load_time_dimension():
        return LoadDimensionOperator(
            task_id='Load_time_dim_table',
            redshift_conn_id='redshift',
            table='time',
            sql_query=SqlQueries.time_table_insert,
            truncate_table=True
        )

    @task
    def run_quality_checks():
        return DataQualityOperator(
            task_id='Run_data_quality_checks',
            redshift_conn_id='redshift',
            test_cases=[
                {'sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL', 'expected_result': 0},
                {'sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL', 'expected_result': 0}
            ]
        )

    @task
    def end_execution():
        return DummyOperator(task_id='End_execution')

    # Define task dependencies
    start = start_execution()
    stage_events_task = stage_events()
    stage_songs_task = stage_songs()
    load_songplays_task = load_songplays()
    load_user_task = load_user_dimension()
    load_song_task = load_song_dimension()
    load_artist_task = load_artist_dimension()
    load_time_task = load_time_dimension()
    quality_checks = run_quality_checks()
    end = end_execution()

    start >> [stage_events_task, stage_songs_task]
    [stage_events_task, stage_songs_task] >> load_songplays_task
    load_songplays_task >> [load_user_task, load_song_task, load_artist_task, load_time_task]
    [load_user_task, load_song_task, load_artist_task, load_time_task] >> quality_checks
    quality_checks >> end

final_project_dag = final_project()