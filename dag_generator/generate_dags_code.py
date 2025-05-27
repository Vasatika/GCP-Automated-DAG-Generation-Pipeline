import os
import json
from datetime import datetime
from jinja2 import Template

# Load all JSON config files
CONFIG_DIR = "json_config_files"
DAG_OUTPUT_DIR = "dags_generated"

# Create output directory if it doesn't exist
os.makedirs(DAG_OUTPUT_DIR, exist_ok=True)

for file in os.listdir(CONFIG_DIR):
    if file.endswith(".json"):
        with open(os.path.join(CONFIG_DIR, file)) as f:
            config = json.load(f)

        dag_id = config["dag_id"]
        ingestion_type = config["ingestion_type"]
        dag_filename = os.path.join(DAG_OUTPUT_DIR, f"{dag_id}.py")

        # Common Airflow and GCP imports for the generated DAG
        imports = """
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from google.cloud import storage
import logging
"""

        # Organizing logic for moving files in GCS after DAG execution
        if ingestion_type == "file-to-bq":
            organizing_logic = """
def move_matching_files(source_prefix, destination_prefix):
    client = storage.Client()
    bucket_name = "{{ config['source_uris'][0].split('/')[2] }}"
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=source_prefix))

    if not blobs:
        raise FileNotFoundError(f"No files found in {source_prefix} to move.")

    for blob in blobs:
        if blob.name.endswith(".csv"):
            dest_blob = blob.name.replace(source_prefix, destination_prefix, 1)
            bucket.copy_blob(blob, bucket, new_name=dest_blob)
            blob.delete()
            logging.info(f"Moved {blob.name} to {dest_blob}")

def on_success(**context):
    move_matching_files("inbound/", "archival/")

def on_failure(**context):
    move_matching_files("inbound/", "failed/")
"""
        else:
            organizing_logic = """
def on_success(**context):
    print("No files to move for BQ-to-BQ pipeline. Success logged.")

def on_failure(**context):
    print("No files to move for BQ-to-BQ pipeline. Failure logged.")
"""

        # Create the final DAG content using Jinja2
        dag_template = Template(f"""{imports}
{organizing_logic}

with DAG(
    dag_id="{dag_id}",
    schedule_interval="{config['schedule_interval']}",
    start_date=datetime.strptime("{config['start_date']}", "%Y-%m-%d"),
    catchup={str(config['catchup'])},
    default_args={config['default_args']},
    tags=["auto-generated"],
) as dag:

""" +
    ("""
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket="{{ config['source_uris'][0].split('/')[2] }}",
        source_objects=["{{ '/'.join(config['source_uris'][0].split('/')[3:]) }}"],
        destination_project_dataset_table="{{ config['destination_project'] }}.{{ config['destination_dataset'] }}.{{ config['staging_table'] }}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect={{ config['autodetect'] }},
        write_disposition="{{ config['write_disposition'] }}",
        field_delimiter=",",
        allow_quoted_newlines=True,
        schema_fields={{ config['schema'] }},
    )
    """ if ingestion_type == "file-to-bq" else f"""
    transform_query = \"\"\"{config['custom_sql']}\"\"\"

    transform_data = BigQueryInsertJobOperator(
        task_id="transform_data",
        configuration={{
            "query": {{
                "query": transform_query,
                "useLegacySql": False,
                "destinationTable": {{
                    "projectId": "{config['destination_project']}",
                    "datasetId": "{config['destination_dataset']}",
                    "tableId": "{config['final_table']}",
                }},
                "writeDisposition": "{config['write_disposition']}",
            }}
        }},
        location="US"
    )
    """) +
    """

    success = PythonOperator(
        task_id="on_success_organize",
        python_callable=on_success
    )

    failure = PythonOperator(
        task_id="on_failure_organize",
        python_callable=on_failure,
        trigger_rule="one_failed"
    )

    """ +
    ("load_to_bq >> [success, failure]" if ingestion_type == "file-to-bq" else "transform_data >> [success, failure]")
        )

        # Render and write the DAG Python file
        rendered_dag = dag_template.render(config=config)
        with open(dag_filename, "w") as f:
            f.write(rendered_dag)

print("All DAGs generated successfully")

# --------------------------------------------------------
# Explanation:
# - Parses each JSON config from json_config_files/
# - Builds a customized DAG using Jinja2 templating
# - Handles two ingestion types: file-to-bq and bq-to-bq
# - Sets up success/failure hooks to move files post-ingestion
# - Writes final Airflow DAGs to dags_generated/ for Composer
# --------------------------------------------------------
