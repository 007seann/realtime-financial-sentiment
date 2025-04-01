from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# Define the DAG
with DAG(
    dag_id="sec_marketSent_predictor_trigger",
    schedule=None,  # This DAG is triggered manually or by another DAG
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    # Task to run the standalone script
    run_sentiment_analysis = BashOperator(
        task_id="run_sentiment_analysis",
        bash_command=(
            "python3 /data/seanchoi/SSPM_local/sec_sent_predictor_local.py"
            "{{ dag_run.conf['csv_file_path'] }} "
            "{{ dag_run.conf['fig_loc'] }} "
            "{{ dag_run.conf['input_path'] }} "
            "{{ dag_run.conf['window'] }}"
        ),
    )
    

