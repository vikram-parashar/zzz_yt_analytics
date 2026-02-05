from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from extract.scrape_agents import scrape_agnents_func

with DAG(
    "setup",
    schedule=None,
) as dag:
    scrape_agents = PythonOperator(
        task_id="scrape_agents_wiki",
        python_callable=scrape_agnents_func,
    )
