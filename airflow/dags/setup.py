from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pipelines.enrich_channel import enrich_channels_func
from pipelines.enrich_video import enrich_videos_func
from pipelines.initial_video_discovery import initial_video_discovery_func
from pipelines.scrape_agents_from_wiki import scrape_agent_func
from sql.init_tables import init_tables_func


with DAG(
    "setup",
    schedule=None,
) as dag:
    enrich_channels = PythonOperator(
        task_id="enrich_channels",
        python_callable=enrich_channels_func,
    )

    enrich_channels
