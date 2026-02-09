from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from extract.scrape_agents import scrape_agnents_func
from load.load_agents import load_agents_func
from sql.init_tables import init_tables_func
from transform.parse_agents import parse_agents_func


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
        python_callable=scrape_agnents_func,
    )
    parse_agents = PythonOperator(
        task_id="transform_to_csv",
        python_callable=parse_agents_func,
    )
    load_agents = PythonOperator(
        task_id="load_agents_to_duckdb",
        python_callable=load_agents_func,
    )
    init_tables >> scrape_agents >> parse_agents >> load_agents
