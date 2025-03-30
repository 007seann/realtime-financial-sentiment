#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres_container:5432/airflow
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

echo "Creating Airflow configuration file at ${AIRFLOW_HOME}/airflow.cfg"
airflow config list > /dev/null

echo "Resetting Airflow migrations"
airflow db reset -y

echo "Initializing Airflow metadata database"
airflow db init

echo "Upgrading Airflow database schema"
airflow db upgrade
