import airflow
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs import HttpToGcsOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(4)}

dag = DAG(
    dag_id="realestate",
    default_args=args,
    description="Real estate",
    schedule_interval="0 0 * * *",
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="get_prices_to_gcs",
    # uses a connection from Airflow - which contains the credentials necessary to access pgsl
    postgres_conn_id="postgres_con",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket=Variable.get('gs_bucket'),
    filename="land_registry_price/{{ds}}/data.json",
    dag=dag,
)

http_to_gcs_op = HttpToGcsOperator(
    task_id="get_currency_to_gcs",
    http_conn_id="currency_con",
    endpoint="convert-currency?date={{ds}}&from=GBP&to=EUR",
    gcs_path="currency/{{ds}}/dates.json",
    delegate_to=None,
    gcs_bucket=Variable.get('gs_bucket'),
    dag=dag,
)

# def upload_file(bucket, filepath):
#     hook = GoogleCloudStorageHook(
#         google_cloud_storage_conn_id="google_cloud_default")
#     hook.upload(
#         bucket=bucket,
#         object=filepath,
#         filename="gs://europe-west1-training-airfl-46f2603e-bucket/dags/build_statistics.py"
#     )


# upload_build_statistics = PythonOperator(
#     task_id="upload_build_statistics",
#     python_callable=upload_file(bucket=Variable.get('gs_bucket'),
#                                 filepath="pyspark/build_statistics.py"),
#     provide_context=True,
#     dag=dag, )

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-fc205e26bebb44fa",
    num_workers=2,
    zone="europe-west1-c",
    dag=dag,
)

compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    # TODO: create operator to upload localfile "build_statistics.py"
    main="gs://europe-west1-training-airfl-46f2603e-bucket/dags/build_statistics.py",
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=[
        "gs://" + Variable.get('gs_bucket') + "/land_registry_price/{{ ds }}/*.json",
        "gs://" + Variable.get('gs_bucket') + "/currency/{{ ds }}/*.json",
        "gs://" + Variable.get('gs_bucket') + "/average_prices/{{ ds }}/"
    ],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-fc205e26bebb44fa",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

load_into_bigquery = DataFlowPythonOperator(
    task_id="land_registry_prices_to_bigquery",
    dataflow_default_options={
        'region': "europe-west1-c",
        'input': "gs://" + Variable.get('gs_bucket') + "/land_registry_price/{{ ds }}/*.json",
        'table': 'emgsilva',
        'dataset': 'emgsilva',
        'project': 'airflowbolcom-fc205e26bebb44fa',
        'bucket': 'europe-west1-training-airfl-46f2603e-bucket',
        'name': '{{ task_instance_key_str }}'
    },
    py_file="gs://europe-west1-training-airfl-46f2603e-bucket/dags/dataflow_job.py",
    dag=dag,
)

[pgsl_to_gcs, http_to_gcs_op] >> load_into_bigquery
[pgsl_to_gcs,
 http_to_gcs_op] >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
