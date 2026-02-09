# The Zenless Zone Zero (ZZZ) Character Popularity & Meta Shift Tracker

### Introduction
The Zenless Zone Zero (ZZZ) Character Popularity & Meta Shift Tracker is a data analytics project that analyzes YouTube content to quantify character popularity and detect meta shifts within Zenless Zone Zero. By leveraging the YouTube Data API v3, the project uses video metadata and engagement signals as a proxy for community interest and in-game relevance.

The system ingests YouTube data on a scheduled basis, transforms it into an analytics-ready schema, and produces time-series insights that highlight emerging trends, declining characters, and the impact of banners or patches. The project demonstrates end-to-end data pipeline design, metric modeling, and analytical reasoning using real-world, high-volume API data.

# agent aliases
why mannual entries @src/data/aliases.py?
first name is written after last name for some character
if "word1 word2" broken and searched some char share common surname (demara), would give wrong results
some known by nickname like rina and lucy
some written differently like yi xuan and yixuan

# quota refernce
https://developers.google.com/youtube/v3/determine_quota_cost
search cost 100, 10k/100 can only search 100 times a day, so discovery would run once manually
video list is cheap cost 1 credit only, will run daily for each video

### TODO
1. manage api exhaustion logic
2. error handing and checks at various places
3. need better data modelling

# setup
* install packages (including local src)
uv sync
uv pip install -e .
* install airflow
bash```
uv pip install "apache-airflow==3.1.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.3/constraints-3.13.txt"
```
* run airflow
bash```
airflow standalone
```
* go to $AIRFLOW_HOME/airflow.cfg , change load_examples to False and then
bash```
airflow db reset
```
* stop airflow standalone and then run 
bash ```
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags && airflow standalone
```


# why 'm using duckdb
duckdb is simplest db choice as an analytics db, unless it struggles no reason to upgrade
simple? yes, just one line
```
con = duckdb.connect("warehouse.duckdb")
```
