from airflow.models import DAG, Variable
from operators.papermill import PapermillOperator
from operators.papermill_minio import PapermillMinioOperator
from airflow.operators.python import PythonOperator
from operators.clean_folder import CleanFolderOperator
from operators.mattermost import MattermostOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import requests
from minio import Minio
import pandas as pd

from dag_schema_data_gouv_fr.utils.geo import improve_geo_data_quality

AIRFLOW_DAG_HOME='/opt/airflow/dags/'
TMP_FOLDER='/tmp/'
DAG_FOLDER = 'dag_schema_data_gouv_fr/'
DAG_NAME = 'schemas_consolidation'

MINIO_URL = Variable.get("minio_url")
MINIO_BUCKET = Variable.get("minio_bucket_opendata")
MINIO_USER = Variable.get("secret_minio_user_opendata")
MINIO_PASSWORD = Variable.get("secret_minio_password_opendata")

# MATTERMOST_ENDPOINT = Variable.get("secret_mattermost_schema_activite")

SCHEMA_CATALOG = 'https://schema.data.gouv.fr/schemas/schemas.json'
# SCHEMA_CATALOG = 'https://raw.githubusercontent.com/geoffreyaldebert/schema-test/master/schemas.json'

API_KEY = Variable.get("secret_api_key_data_gouv")
API_URL = "https://demo.data.gouv.fr/api/1/"
# API_URL = "https://www.data.gouv.fr/api/1/"

default_args = {
   'email': ['sixte.de-maupeou@data.gouv.fr'],
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
                filename = 'https://' + MINIO_URL + '/' + MINIO_BUCKET + '/datagouv_test/schemas_consolidation/' + \
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
                    "datagouv_test/schemas_consolidation/" + last_conso + "/liste_erreurs/" + \
                    'liste_erreurs-'+s['name'].replace('/','_') + '.csv', \
                    TMP_FOLDER + DAG_FOLDER + 'liste_erreurs-'+s['name'].replace('/','_') + '.csv'
                )

                message += '\n- Schéma ***{}***\n - Ressources déclarées : {}'.format(s['title'], nb_declares)
                
                if(nb_suspectes != 0): message += '\n - Ressources suspectées : {}'.format(nb_suspectes)
                
                message += '\n - Ressources valides : {} \n - [Liste des ressources non valides]({})\n'.format(nb_valides, \
                    'https://object.files.data.gouv.fr/opendata/datagouv_test/schemas_consolidation/' + last_conso + '/liste_erreurs/' + 'liste_erreurs-'+s['name'].replace('/','_') + '.csv' 
                )


            except:
                print('No report for {}'.format(s['name']))
                pass

    # publish_mattermost = MattermostOperator(
    #     task_id="publish_result",
    #     mattermost_endpoint=MATTERMOST_ENDPOINT,
    #     text=message
    # )
    # publish_mattermost.execute(dict())

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 6 * * TUE',
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=60),
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
    
    run_nb_consolidation = PapermillOperator(
        task_id="run_notebook_schemas_consolidation",
        input_nb=AIRFLOW_DAG_HOME + DAG_FOLDER + "notebooks/schemas_consolidation/schemas_consolidation.ipynb",
        output_nb='{{ ds }}' + "_schemas_consolidation.ipynb",
        parameters=shared_notebooks_params
    )

    schema_irve_path = os.path.join(tmp_folder, 'consolidated_data', 'etalab_schema-irve')
    schema_irve_cols = {
        'xy_coords': 'coordonneesXY',
        'code_insee': 'code_insee_commune',
        'adress': 'adresse_station',
        'longitude': 'longitude',
        'latitude': 'latitude'
    }

    geodata_quality_improvement = PythonOperator(
        task_id="geodata_quality_improvement",
        python_callable=lambda schema_path: improve_geo_data_quality({os.path.join(schema_path, filename): schema_irve_cols for filename in os.listdir(schema_path)}),
        op_args=[schema_irve_path]
    )

    upload_consolidation = PapermillMinioOperator(
        task_id="upload_consolidated_datasets",
        input_nb=AIRFLOW_DAG_HOME + DAG_FOLDER + "notebooks/schemas_consolidation/consolidation_upload.ipynb",
        output_nb='{{ ds }}' + "_consolidation_upload.ipynb",
        tmp_path=TMP_FOLDER+DAG_FOLDER + '{{ ds }}' + "/",
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET,
        minio_user=MINIO_USER,
        minio_password=MINIO_PASSWORD,
        minio_output_filepath='datagouv_test/schemas_consolidation/' + '{{ ds }}' + "/",
        parameters={
            **shared_notebooks_params,
            "OUTPUT_DATA_FOLDER": TMP_FOLDER + DAG_FOLDER + '{{ ds }}' + "/output/"
        }
    )

    notification_synthese = PythonOperator(
        task_id="notification_synthese",
        python_callable=notification_synthese,
        templates_dict={
            "TODAY": '{{ ds }}'
        },
    )
    

    
    clean_previous_outputs >> run_nb_consolidation >> geodata_quality_improvement >> upload_consolidation >> notification_synthese
