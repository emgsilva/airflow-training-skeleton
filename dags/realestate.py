import airflow
from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs import HttpToGcsOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(4)}

dag = DAG(
    dag_id="realestate",
    default_args=args,
    description="Real estate",
    schedule_interval="0 0 * * *",
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pgsl_to_gcs",
    # uses a connection from Airflow - which contains the credentials necessary to access pgsl
    postgres_conn_id="postgres_con",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket=Variable.get('gs_bucket'),
    filename="land_registry_price/{{ds}}/data.json",
    dag=dag,
)

http_to_gcs_op = HttpToGcsOperator(
    task_id="http_to_gcs_op",
    http_conn_id="currency_con",
    endpoint="convert-currency?date={{ds}}&from=GBP&to=EUR",
    gcs_path="currency/{{ds}}/dates.json",
    delegate_to=None,
    gcs_bucket=Variable.get('gs_bucket'),
    dag=dag,
)

[pgsl_to_gcs, http_to_gcs_op]
