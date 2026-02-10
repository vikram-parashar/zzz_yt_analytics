#!/bin/bash

set -a && source ./.env && set +a

export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags 
airflow standalone
