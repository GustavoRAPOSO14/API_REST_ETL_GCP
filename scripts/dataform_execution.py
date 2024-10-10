from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator
)
import pendulum
from google.oauth2 import service_account
from google.cloud import bigquery
import logging
import time
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/airflow/logical-essence-433414-r5-6b6fcca101ce.json"



key_path = "/home/airflow/logical-essence-433414-r5-6b6fcca101ce.json"



credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

def connectionGoogleBigQuery():

    client_bq = bigquery.Client(credentials=credentials, project=credentials.project_id)

    return client_bq



PROJECT_ID = "logical-essence-433414-r5"
REGION = "us-east4"
REPOSITORY_ID = "datalakehouse"
WORKSPACE_ID = "queries_teste"

def truncate_table(bigquery_dataset_trusted, bigquery_dataset_tabela):
    
    client_bq = connectionGoogleBigQuery()

    try:
        logging.info(f"Truncating table {bigquery_dataset_trusted}.{bigquery_dataset_tabela}.")
        bq_query_truncate_trusted = ("TRUNCATE TABLE " + PROJECT_ID + "." + bigquery_dataset_trusted + "." + bigquery_dataset_tabela + ";")
        query_job = client_bq.query(bq_query_truncate_trusted)
        query_job.result()
        time.sleep(15)
        logging.info(f"Successfully truncated table {bigquery_dataset_trusted}.{bigquery_dataset_tabela}.")
    except Exception as truncate_error:
        logging.error(f"An error has occured during TRUNCATE process in {bigquery_dataset_trusted}.{bigquery_dataset_tabela}. Error: {truncate_error}")
        raise truncate_error
    
def run():
    truncate_table('LAND_FLATFILE', 'api_escolas')
    truncate_table('LAND_FLATFILE', 'api_estatisticas')
    truncate_table('LAND_FLATFILE', 'api_infraestrutura')





def check_compilation_result(**kwargs):
    compilation_result = kwargs['task_instance'].xcom_pull(task_ids='create-compilation-result')
    if not compilation_result or 'name' not in compilation_result:
        raise ValueError("Compilation result is None or does not contain 'name'.")

with DAG(
        dag_id="dataform_execution",
        schedule_interval=None,
        start_date=pendulum.datetime(2024, 9, 27, tz="America/Sao_Paulo"),
        catchup=False,
        is_paused_upon_creation=True,
        tags=["Dataform", "api"]
) as dag:

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create-compilation-result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "workspace": (
                f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
                f"workspaces/{WORKSPACE_ID}"
            ),
        },
    )

    check_result = PythonOperator(
        task_id="check-compilation-result",
        python_callable=check_compilation_result,
        provide_context=True,
    )

    get_compilation_result = DataformGetCompilationResultOperator(
        task_id="get-compilation-result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result_id=(
            "{{ task_instance.xcom_pull(task_ids='create-compilation-result')['name'].split('/')[-1] }}"
        ),
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull(task_ids='create-compilation-result')['name'] }}"
        },
    )

    truncate = PythonOperator(
        task_id="truncate_land",
        python_callable=run,
        provide_context=True,
    )

    # Define a ordem de execução
    create_compilation_result >> check_result >> get_compilation_result >> create_workflow_invocation >> truncate