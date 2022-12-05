

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.operators.python import BranchPythonOperator
from random import uniform

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from airflow.utils.task_group import TaskGroup

from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator


########################
# DataprocSubmitPySparkJobOperator 

pyspark_job = DataprocSubmitPySparkJobOperator(
     task_id='pyspark_job',
     project_id='proyecto-grupal-369315',
     main='gs://spark-cluster-henry/pyspark/ETLbusiness.py',
     cluster_name='spark-cluster-henry',
     dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
     region='us-east1'
 ).generate_job()



########################


default_args = {
    'owner': 'Matias B',
    'start_date': days_ago(1)
}

dag_args = {
    'dag_id': 'ETL_Business_spark',
    'schedule_interval': '@daily',
    'catchup': False,
    'default_args': default_args
}


with DAG(**dag_args) as dag:
    
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id='proyecto-grupal-369315',
        cluster_name='spark-cluster-henry',
        num_workers=2,
        storage_bucket='spark-cluster-henry',
        region='us-east1'
    )

# EJECUTAR PYSPARK JOB
   
    cleanup = DataprocSubmitJobOperator(
        task_id='Cleanup',
        project_id='proyecto-grupal-369315',
        region='us-east1',
        job=pyspark_job
        
    )


# DELETE CLUSTER TASK 
    delete_cluster = DataprocDeleteClusterOperator(
       task_id='delete_cluster',
       project_id='regal-oasis-291423',
       cluster_name='sparkcluster-987',
       region='us-east1',
       trigger_rule='all_done'
   )


# DEPENDENCIAS
(
    create_cluster
    >> 
    cleanup
    >>
    delete_cluster
)