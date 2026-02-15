from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pipelines.daily_video_discovery import daily_video_discovery_func
from pipelines.enrich_channel import enrich_channels_func
from pipelines.enrich_video import enrich_videos_func

with DAG(
    "daily_pipeline_run",
    schedule="@daily",
) as dag:
    daily_video_discovery = PythonOperator(
        task_id="daily_video_discovery",
        python_callable=daily_video_discovery_func,
    )

    enrich_videos = PythonOperator(
        task_id="enrich_videos",
        python_callable=enrich_videos_func,
    )

    enrich_channels = PythonOperator(
        task_id="enrich_channels",
        python_callable=enrich_channels_func,
    )

    daily_video_discovery >> enrich_videos >> enrich_channels
