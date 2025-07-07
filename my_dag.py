from airflow import models
from airflow.utils.dates import days_ago
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.operators.python import PythonOperator

import pandas as pd
from google.cloud import pubsub_v1
import time


PROJECT_ID = "learn-gcloud-462613"
BUCKET = "us-central1-dev-f857e1ca-bucket"
REGION = "us-central1"
PUBSUB_TOPIC_ID = "traffic-weather-simulated"
BQ_OUTPUT_TABLE = f"{PROJECT_ID}:final.traffic_weather"
DATAFLOW_SCRIPT = f"gs://{BUCKET}/dags/dataflow_pubsub_to_bq.py"
CSV_PATH = "gs://my-file-dump/metro_traffic_volume_small.csv"


default_args = {
    "start_date": days_ago(0),
    "retries": 0,
}


def csv_to_pubsub():
    df = pd.read_csv(CSV_PATH)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_ID)

    for index, row in df.iterrows():
        row_data = row.to_json().encode("utf-8")
        future = publisher.publish(topic_path, row_data)
        print(f"Published row {index} with message ID: {future.result()}")
        time.sleep(1)


with models.DAG(
    dag_id="streaming_pubsub_to_bq_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    publish_to_pubsub = PythonOperator(
        task_id="publish_to_pubsub",
        python_callable=csv_to_pubsub,
    )

    run_dataflow_pipeline = BeamRunPythonPipelineOperator(
        task_id="run_dataflow_pipeline",
        runner="DataflowRunner",
        py_file=DATAFLOW_SCRIPT,
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "temp_location": f"gs://{BUCKET}/temp",
            "input_topic": f"projects/{PROJECT_ID}/topics/{PUBSUB_TOPIC_ID}",
            "output_table": BQ_OUTPUT_TABLE,
        },
        py_interpreter="python3",
    )

    [publish_to_pubsub, run_dataflow_pipeline]
