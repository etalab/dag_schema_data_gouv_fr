from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from operators.clean_folder import CleanFolderOperator
from operators.mattermost import MattermostOperator
from operators.python_minio import PythonMinioOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import requests
from minio import Minio
import pandas as pd

from dag_schema_data_gouv_fr.utils.geo import improve_geo_data_quality
from dag_schema_data_gouv_fr.notebooks.schemas_consolidation.schemas_consolidation import run_schemas_consolidation
from dag_schema_data_gouv_fr.notebooks.schemas_consolidation.consolidation_upload import run_consolidation_upload

AIRFLOW_DAG_HOME='/opt/airflow/dags/'
TMP_FOLDER='/tmp/'
DAG_FOLDER = 'dag_schema_data_gouv_fr/'
DAG_NAME = 'schemas_consolidation'

MINIO_URL = Variable.get("MINIO_URL")
MINIO_BUCKET = Variable.get("MINIO_BUCKET_OPENDATA")
MINIO_USER = Variable.get("SECRET_MINIO_USER_OPENDATA")
MINIO_PASSWORD = Variable.get("SECRET_MINIO_PASSWORD_OPENDATA")

MATTERMOST_ENDPOINT = Variable.get("MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE")

SCHEMA_CATALOG = 'https://schema.data.gouv.fr/schemas/schemas.json'
# SCHEMA_CATALOG = 'https://raw.githubusercontent.com/geoffreyaldebert/schema-test/master/schemas.json'

API_KEY = Variable.get("DATAGOUV_SECRET_API_KEY")
# API_URL = "https://demo.data.gouv.fr/api/1/"
API_URL = "https://www.data.gouv.fr/api/1/"

default_args = {
   'email': ['geoffrey.aldebert@data.gouv.fr'],
   'email_on_failure': True
}


def notification_synthese(**kwargs):
    templates_dict = kwargs.get("templates_dict")

    client = Minio(
        MINIO_URL,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True
    )

    last_conso = templates_dict['TODAY']

    r = requests.get('https://schema.data.gouv.fr/schemas/schemas.json')
    schemas = r.json()['schemas']
    
    message = ':mega: *Rapport sur la consolidation des données répondant à un schéma.*\n'

    for s in schemas:
        if(s['schema_type'] == 'tableschema'):
            try:
                filename = 'https://' + MINIO_URL + '/' + MINIO_BUCKET + '/datagouv/schemas_consolidation/' + \
                    last_conso + \
                    '/output/ref_tables/' + \
                    'ref_table_' + s['name'].replace('/','_') + '.csv'
                df = pd.read_csv(filename)
                print(df.shape[0])
                nb_declares = df[df['resource_found_by'] == '1 - schema request'].shape[0]
                nb_suspectes = df[df['resource_found_by'] != '1 - schema request'].shape[0]
                nb_valides = df[df['is_valid_one_version'] == True].shape[0]
                df = df[df['is_valid_one_version'] == False]
                print(df.shape[0])
                df = df[['dataset_id', 'resource_id', 'dataset_title', 'resource_title', 'dataset_page', 'resource_url', 'resource_found_by']]
                df['schema_name'] = s['title']
                df['schema_id'] = s['name']
                df['validata_report'] = 'https://validata.etalab.studio/table-schema?input=url&url=' + df['resource_url'] + \
                    '&schema_url=' + s['schema_url']
                df.to_csv(TMP_FOLDER + DAG_FOLDER + 'liste_erreurs-'+s['name'].replace('/','_') + '.csv')
                client.fput_object(
                    "opendata", \
                    "datagouv/schemas_consolidation/" + last_conso + "/liste_erreurs/" + \
                    'liste_erreurs-'+s['name'].replace('/','_') + '.csv', \
                    TMP_FOLDER + DAG_FOLDER + 'liste_erreurs-'+s['name'].replace('/','_') + '.csv'
                )

                message += '\n- Schéma ***{}***\n - Ressources déclarées : {}'.format(s['title'], nb_declares)
                
                if(nb_suspectes != 0): message += '\n - Ressources suspectées : {}'.format(nb_suspectes)
                
                message += '\n - Ressources valides : {} \n - [Liste des ressources non valides]({})\n'.format(nb_valides, \
                    'https://object.files.data.gouv.fr/opendata/datagouv/schemas_consolidation/' + last_conso + '/liste_erreurs/' + 'liste_erreurs-'+s['name'].replace('/','_') + '.csv' 
                )


            except:
                print('No report for {}'.format(s['name']))
                pass

    publish_mattermost = MattermostOperator(
        task_id="publish_result",
        mattermost_endpoint=MATTERMOST_ENDPOINT,
        text=message
    )
    publish_mattermost.execute(dict())

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=120),
    tags=['schemas','irve','consolidation','datagouv'],
    default_args=default_args,
) as dag:
    
    clean_previous_outputs = CleanFolderOperator(
        task_id="clean_previous_outputs",
        folder_path=TMP_FOLDER + DAG_FOLDER
    )

    tmp_folder = TMP_FOLDER + DAG_FOLDER + '{{ ds }}' + "/"

    shared_notebooks_params = {
            "msgs": "Ran from Airflow " + '{{ ds }}' + "!",
            "WORKING_DIR": AIRFLOW_DAG_HOME + DAG_FOLDER + 'notebooks/',
            "TMP_FOLDER": tmp_folder,
            "API_KEY": API_KEY,
            "API_URL": API_URL,
            "DATE_AIRFLOW": '{{ ds }}',
            "SCHEMA_CATALOG": SCHEMA_CATALOG
        }

    working_dir = AIRFLOW_DAG_HOME + DAG_FOLDER + 'notebooks/'
    date_airflow = '{{ ds }}'

    run_consolidation = PythonOperator(
        task_id="run_notebook_schemas_consolidation",
        python_callable=run_schemas_consolidation,
        op_args=(API_URL, working_dir, tmp_folder, date_airflow, SCHEMA_CATALOG)
    )

    schema_irve_path = os.path.join(tmp_folder, 'consolidated_data', 'etalab_schema-irve')
    schema_irve_cols = {
        'xy_coords': 'coordonneesXY',
        'code_insee': 'code_insee_commune',
        'adress': 'adresse_station',
        'longitude': 'consolidated_longitude',
        'latitude': 'consolidated_latitude'
    }

    geodata_quality_improvement = PythonOperator(
        task_id="geodata_quality_improvement",
        python_callable=lambda schema_path: improve_geo_data_quality({os.path.join(schema_path, filename): schema_irve_cols for filename in os.listdir(schema_path)}) if schema_path is not None else None,
        op_args=[schema_irve_path]
    )

    output_data_folder = TMP_FOLDER + DAG_FOLDER + '{{ ds }}' + "/output/"
    upload_consolidation = PythonMinioOperator(
        task_id="upload_consolidated_datasets",
        tmp_path=tmp_folder,
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET,
        minio_user=MINIO_USER,
        minio_password=MINIO_PASSWORD,
        minio_output_filepath='datagouv/schemas_consolidation/' + '{{ ds }}' + "/",
        python_callable=run_consolidation_upload,
        op_args=(API_URL, API_KEY, tmp_folder, working_dir, date_airflow, SCHEMA_CATALOG, output_data_folder)
    )

    notification_synthese = PythonOperator(
        task_id="notification_synthese",
        python_callable=notification_synthese,
        templates_dict={
            "TODAY": '{{ ds }}'
        },
    )
    

    
    clean_previous_outputs >> run_consolidation >> geodata_quality_improvement >> upload_consolidation >> notification_synthese