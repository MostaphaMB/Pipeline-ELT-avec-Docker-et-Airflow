from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

#Import  tâches Data Warehouse ---
from dwh import staging_table, core_table

import pendulum

from datetime import datetime, timedelta

from api.videos_status import (
    get_playlist_id,
    get_video_ids,
    extract_video_details,
    save_to_json,
)


# Morocco timezone
local_tz = pendulum.timezone("Africa/Casablanca")


# Default arguments
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email": "data@engineers.com",
    "email_on_failure": False,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}


# DAG : Produce JSON
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw YouTube data",
    schedule="0 14 * * *",   # Every day at 14:00 Morocco time
    catchup=False,
    tags=["youtube", "etl", "json"],
) as dag_produce:

    # Tasks
    playlist_id = get_playlist_id()

    video_ids = get_video_ids(playlist_id)

    extract_data = extract_video_details(video_ids)

    save_to_json_task = save_to_json(extract_data)



    # Dependencies
    (
        playlist_id >> video_ids >> extract_data >> save_to_json_task
    )

# DAG 3:
with DAG(
    dag_id="update_staging_json",
    default_args=default_args,
    description="DAG to update staging JSON file with raw YouTube data",
    schedule="0 14 * * *",   # Every day at 14:00 Morocco time
    catchup=False,
    tags=["youtube", "etl", "json"],
) as dag_produce:

    # Tasks
    playlist_id = get_playlist_id()

    video_ids = get_video_ids(playlist_id)

    extract_data = extract_video_details(video_ids)

    save_to_json_task = save_to_json(extract_data)



    # Dependencies
    (
        playlist_id >> video_ids >> extract_data >> save_to_json_task
    )



#DAG 3 : YouTube Data Warehouse (DWH) ---
with DAG(
    dag_id="youtube_dwh_update",
    default_args=default_args,
    description="DAG to update Staging and Core tables in Postgres",
    schedule="30 14 * * *",  # Se lance à 14:30 (après le JSON)
    catchup=False,
    tags=["youtube", "dwh", "postgres"],
) as dag_dwh:

    # Appel de tâches @task
    task_staging = staging_table()
    task_core = core_table()

    # Dépendance : On met à jour le Staging AVANT le Core
    task_staging >> task_core