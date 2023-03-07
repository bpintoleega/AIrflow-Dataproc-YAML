from pyspark.sql import SparkSession
from google.cloud import storage
from datetime import datetime
import sys

spark = SparkSession \
    .builder \
    .appName("Hello World teste") \
    .getOrCreate()

# Recebendo parametros do Airflow (que vem do YAML).
PROJECT_ID = sys.argv[1]
NAME_BUCKET_SCRIPTS = sys.argv[2]
NAME_DATA_PATH = sys.argv[3]
CLUSTER_NAME = sys.argv[4]
LOG_NAMEFILE = sys.argv[5]

# Função para escrever um log do horário exato em que o job foi executado no GCS
#def write_log(bucket_name, blob_name):
#    storage_client = storage.Client()
#    bucket = storage_client.bucket(bucket_name)
#    blob = bucket.blob(blob_name)
#    agora = datetime.now()
#    str_agora = agora.strftime("%m/%d/%Y, %H:%M:%S")
#    blob.upload_from_string(str_agora)

#write_log(NAME_BUCKET_SCRIPTS, LOG_NAMEFILE)

