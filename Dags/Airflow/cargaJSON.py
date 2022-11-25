from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    "owner": "Matias",
    'start_date':days_ago(7)
}

dag_args = {
    'dag_id': 'CargarJSON',
    'schedule_interval':'@daily',
    'catchup': False,
    'default_args': default_args
}

with DAG(**dag_args) as dag:
  
    cargar_chekin = GCSToBigQueryOperator(
        task_id = 'cargar_chekin',
        bucket ='testingnuevo',
        source_objects='checkin.json',
        source_format = 'NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table = 'proyecto-grupal-369315.Henry.checkin'
        #create_disposition='CREATE_IF_NEEDED'
        #bigquery_conn_id= 'google_cloud_default',
        #google_cloud_storage_conn_id= 'google_cloud_default'
    )

    cargar_tip = GCSToBigQueryOperator(
        task_id = 'cargar_tip',
        bucket ='testingnuevo',
        source_objects='tip.json',
        source_format = 'NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table = 'proyecto-grupal-369315.Henry.tip'
        #create_disposition='CREATE_IF_NEEDED'
        #bigquery_conn_id= 'google_cloud_default',
        #google_cloud_storage_conn_id= 'google_cloud_default'
    )
    



cargar_chekin >> cargar_tip