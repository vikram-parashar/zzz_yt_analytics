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
    init_tables = PythonOperator(
        task_id="init_duckdb_tables",
        python_callable=init_tables_func,
    )

    scrape_agents = PythonOperator(
        task_id="scrape_agents_wiki",
        python_callable=scrape_agent_func,
    )

    initial_video_discovery = PythonOperator(
        task_id="initial_video_discovery",
        python_callable=initial_video_discovery_func,
    )

    enrich_videos = PythonOperator(
        task_id="enrich_videos",
        python_callable=enrich_videos_func,
    )

    enrich_channels = PythonOperator(
        task_id="enrich_channels",
        python_callable=enrich_channels_func,
    )

    init_tables >> scrape_agents >> initial_video_discovery
    initial_video_discovery >> enrich_videos
    initial_video_discovery >> enrich_channels
