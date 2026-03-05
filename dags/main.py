from airflow import DAG
import pendulum 
from datetime import datetime, timedelta 
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json    
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality_check

local_tz = pendulum.timezone("America/Bogota")

default_args = {
    "owner": "yuletsypabon",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "yuletsyp.pabonf@gmail.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 2, 18, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description="A DAG to extract YouTube video statistics and save them to a JSON file",
    schedule = "0 14 * * *", # runs daily at 2:00 PM
    catchup=False,
) as dag_produce:
    
    
    #Define tasks  
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)
    
    triger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )
    
    # Define task dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task >> triger_update_db
    

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description="A DAG to process JSON file and insert/update data in the staging and core tables",
    schedule = None, # runs daily at 2:00 PM
    catchup=False,
) as dag_update:
    #Define tasks 
    update_staging = staging_table()
    update_core = core_table() 
    
    trigger_data_quality_check = TriggerDagRunOperator(
        task_id="trigger_data_quality_check",
        trigger_dag_id="data_quality_check",
    )
    
    # Define task dependencies
    update_staging >> update_core >> trigger_data_quality_check

# Variables for data quality check DAG
staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id='data_quality_check',
    default_args=default_args,
    description="A DAG to run data quality checks using Soda",
    schedule = None, # runs daily at 2:00 PM
    catchup=False,
) as dag_quality_check:
    #Define tasks 
    soda_check_staging = yt_elt_data_quality_check(staging_schema)
    soda_check_core = yt_elt_data_quality_check(core_schema)
    # Define task dependencies
    soda_check_staging >> soda_check_core