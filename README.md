# 🚦 GCP Streaming Data Pipeline (CSV → Pub/Sub → Dataflow → BigQuery)

This project demonstrates a real-time streaming data pipeline using **Google Cloud Platform** services: **Pub/Sub**, **Dataflow**, **BigQuery**, and **Cloud Composer**.

We simulate streaming behavior by publishing rows from a CSV file (stored in Cloud Storage) to Pub/Sub at intervals. This mimics how real-time data (like traffic, logs, or IoT events) would flow into a system continuously.

---

## 📌 Project Overview

**Objective**: Learn to build a scalable, streaming data pipeline on GCP by simulating real-time data flow using historical traffic and weather data.

**Data Flow**:
1. Read a CSV file from GCS (`my-file-dump` bucket).
2. Publish rows one-by-one into a Pub/Sub topic.
3. Use a Dataflow streaming job to read messages from Pub/Sub.
4. Write the transformed data into a BigQuery table.


---

## 🔧 Setup Instructions

### 1. Prerequisites
- Google Cloud project
- Cloud Composer environment
- Enabled APIs:
  - Pub/Sub
  - Dataflow
  - BigQuery
  - GCS

### 2. Resources to Create
- Pub/Sub Topic: `traffic-weather-simulated`
- BigQuery Dataset: `final`
- Cloud Storage Buckets:
  - `my-file-dump` → holds your CSV file
  - `your-composer-bucket` → used for DAGs, staging, and temp files

### 3. Upload Files
- Upload the DAG (`streaming_pipeline_dag.py`) to your Composer environment's `dags/` folder.
- Upload the Beam script (`dataflow_pubsub_to_bq.py`) to GCS under your Composer bucket, e.g., `gs://your-composer-bucket/dags/`.

---

## 🚀 Running the Pipeline

Trigger the Airflow DAG manually from the Cloud Composer UI. The DAG will:

1. Start the Dataflow streaming job.
2. Begin publishing rows from the CSV into Pub/Sub with a small delay between each row.

This simulates streaming input and allows Dataflow to continuously read and write data to BigQuery.

---

## 📊 BigQuery Table Schema

`final.traffic_weather` table fields:

| Column              | Type     |
|---------------------|----------|
| traffic_volume      | INTEGER  |
| holiday             | STRING   |
| temp                | FLOAT    |
| rain_1h             | FLOAT    |
| snow_1h             | FLOAT    |
| clouds_all          | INTEGER  |
| weather_main        | STRING   |
| weather_description | STRING   |
| date_time           | STRING   |

---

## 🛑 Important Note on Stopping the Job

This project uses a **static CSV file with ~100 rows** for testing.  
Because we’re using **Dataflow in streaming mode**, the job **will continue running indefinitely** unless you stop it.

👉 **Manually stop the Dataflow job** via the GCP Console after the pipeline finishes publishing all rows.

To stop:
1. Go to **Dataflow > Jobs** in the GCP Console.
2. Click on the running job.
3. Click **Stop** and choose **Cancel** (not Drain).

---

## 🔐 IAM Roles Required

Make sure the Cloud Composer environment’s service account has the following roles:

- `roles/pubsub.publisher`
- `roles/pubsub.subscriber`
- `roles/dataflow.admin`
- `roles/dataflow.worker`
- `roles/bigquery.dataEditor`
- `roles/storage.admin`

---

## 🧪 Local Development (Optional)

If you want to test the publisher locally:

```bash
pip install -r requirements.txt
