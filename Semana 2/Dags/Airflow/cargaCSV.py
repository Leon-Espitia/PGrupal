from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    "owner": "Matias",
    'start_date':days_ago(7)
}

dag_args = {
    'dag_id': 'CargarCSV',
    'schedule_interval':'@daily',
    'catchup': False,
    'default_args': default_args
}

with DAG(**dag_args) as dag:

    cargar_income = GCSToBigQueryOperator(
        task_id = 'cargar_income',
        bucket ='testingnuevo',
        source_objects='Limpios/U.S.A_household_media_income.csv',
        source_format = 'CSV',
        destination_project_dataset_table = 'proyecto-grupal-369315.Henry.Income'
        #create_disposition='CREATE_IF_NEEDED'
        #bigquery_conn_id= 'google_cloud_default',
        #google_cloud_storage_conn_id= 'google_cloud_default'
    )

    cargar_population = GCSToBigQueryOperator(
        task_id = 'cargar_population',
        bucket ='testingnuevo',
        source_objects='Limpios/U.S.A_total_population.csv',
        source_format = 'CSV',
        destination_project_dataset_table = 'proyecto-grupal-369315.Henry.population'
        #create_disposition='CREATE_IF_NEEDED
        #bigquery_conn_id= 'google_cloud_default',
        #google_cloud_storage_conn_id= 'google_cloud_default'
    )
    cargar_business = GCSToBigQueryOperator(
        task_id = 'cargar_business',
        bucket ='testingnuevo',
        source_objects='Limpios/business.csv',
        source_format = 'CSV',
        destination_project_dataset_table = 'proyecto-grupal-369315.Henry.business'
        #create_disposition='CREATE_IF_NEEDED'
        #bigquery_conn_id= 'google_cloud_default',
        #google_cloud_storage_conn_id= 'google_cloud_default'
    )

    cargar_datos4 = GCSToBigQueryOperator(
        task_id = 'cargar_datos4',
        bucket ='testingnuevo',
        source_objects='user.json',
        source_format = 'NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table = 'proyecto-grupal-369315.Henry.user'
        #create_disposition='CREATE_IF_NEEDED'
        #bigquery_conn_id= 'google_cloud_default',
        #google_cloud_storage_conn_id= 'google_cloud_default'
    )





cargar_income >> cargar_population >> cargar_business >> cargar_datos4