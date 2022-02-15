from airflow.models import DAG, Variable
from operators.papermill_minio import PapermillMinioOperator
from operators.mattermost import MattermostOperator
from operators.mail_datagouv import MailDatagouvOperator
from operators.clean_folder import CleanFolderOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import json

AIRFLOW_DAG_HOME='/opt/airflow/dags/'
TMP_FOLDER='/tmp/'
DAG_FOLDER = 'dag_consolidation_schemas/'
DAG_NAME = 'consolidation_schemas'

MINIO_URL = Variable.get("minio_url")
MINIO_BUCKET = Variable.get("minio_bucket_opendata")
MINIO_USER = Variable.get("secret_minio_user_opendata")
MINIO_PASSWORD = Variable.get("secret_minio_password_opendata")

API_KEY = Variable.get("secret_api_key_data_gouv")
API_URL = "https://www.data.gouv.fr/api/1/"

default_args = {
   'email': ['geoffrey.aldebert@data.gouv.fr'],
   'email_on_failure': True
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 6 1,8,15,22,28 * *',
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=60),
    tags=['schemas','irve','consolidation','datagouv'],
    default_args=default_args,
) as dag:
    
    clean_previous_outputs = CleanFolderOperator(
        task_id="clean_previous_outputs",
        folder_path=TMP_FOLDER+DAG_FOLDER
    )
    
    run_nb = PapermillMinioOperator(
        task_id="run-notebook-consolidation-schemas",
        input_nb=AIRFLOW_DAG_HOME+DAG_FOLDER+"consolidation_tableschema.ipynb",
        output_nb='{{ ds }}'+".ipynb",
        tmp_path=TMP_FOLDER+DAG_FOLDER+'{{ ds }}'+"/",
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET,
        minio_user=MINIO_USER,
        minio_password=MINIO_PASSWORD,
        minio_output_filepath='datagouv/consolidation_schema/'+'{{ ds }}'+"/",
        parameters={
            "msgs": "Ran from Airflow "+'{{ ds }}'+"!",
            "WORKING_DIR": AIRFLOW_DAG_HOME+DAG_FOLDER,
            "TMP_FOLDER": TMP_FOLDER+DAG_FOLDER+'{{ ds }}'+"/",
            "OUTPUT_DATA_FOLDER": TMP_FOLDER+DAG_FOLDER+'{{ ds }}'+"/output/",
            "API_KEY": API_KEY,
            "API_URL": API_URL,
            "DATE_AIRFLOW": '{{ ds }}'
        }
    )
    
    clean_previous_outputs >> run_nb
