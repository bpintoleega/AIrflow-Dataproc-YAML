import datetime
from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
   DataprocCreateClusterOperator,
   DataprocSubmitJobOperator,
   DataprocDeleteClusterOperator,
   ClusterGenerator
)
import sys
import yaml
from google.cloud import storage

with open('/home/airflow/gcs/dags/args/config.yaml', 'r') as blob:
    data_paths = yaml.safe_load(blob)

# Constantes
#data_paths['ambiente']
PROJECT_ID = data_paths['project_id']
NAME_BUCKET_SCRIPTS = data_paths['bucket_name']
NAME_DATA_PATH = data_paths['name_data_path']
CLUSTER_NAME =  data_paths['cluster']['name']
REGION = data_paths['region']
ZONE = data_paths['zone']
LOG_NAMEFILE = data_paths['log_filename']
JOB_BUCKET = data_paths['bucket_padrao']

PYSPARK_PATH_SCRIPT_EXTRATOR = 'gs://' + NAME_BUCKET_SCRIPTS + '/python/helloworld.py'

CLUSTER_CONFIG = ClusterGenerator(
    project_id= data_paths['project_id'],
    region= REGION,
    cluster_name= CLUSTER_NAME,
    num_workers= data_paths['cluster']['num_workers'],
    storage_bucket= data_paths['cluster']['storage_bucket'],
    num_masters= data_paths['cluster']['num_masters'],
    master_machine_type= data_paths['cluster']['master_machine_type'],
    master_disk_type= data_paths['cluster']['master_disk_type'],
    master_disk_size= data_paths['cluster']['master_disk_size'],
    worker_machine_type= data_paths['cluster']['worker_machine_type'],
    worker_disk_type= data_paths['cluster']['worker_disk_type'],
    worker_disk_size= data_paths['cluster']['worker_disk_size'],
    idle_delete_ttl= data_paths['cluster']['idle_delete_ttl']
    # Removido para teste -> service_account= data_paths['cluster']['service_account']
).make()

PYSPARK_JOB_EXTRATOR = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_PATH_SCRIPT_EXTRATOR,
        "args" : [PROJECT_ID,NAME_BUCKET_SCRIPTS,NAME_DATA_PATH, CLUSTER_NAME, LOG_NAMEFILE]
    }
}

default_dag_args = {
    'start_date': datetime.datetime(2023,3,6),
    'catchup': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False
}

with models.DAG(
    "dataproc_exames_zerados",
    schedule_interval = '@daily', # Executa todos os domingos as 2 da manha
    default_args=default_dag_args,
    ) as dag:

    print(CLUSTER_CONFIG)
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task_extrator = DataprocSubmitJobOperator(
        task_id="job_extrator", 
        job=PYSPARK_JOB_EXTRATOR,
        region=REGION, 
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    create_cluster >> pyspark_task_extrator >> delete_cluster