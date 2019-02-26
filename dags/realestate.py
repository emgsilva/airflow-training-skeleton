import airflow
from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(4)}

dag = DAG(
    dag_id="realestate",
    default_args=args,
    description="Real estate",
    schedule_interval="0 0 * * *",
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pgsl_to_gcs",
    # uses a connection
    postgres_conn_id="postgres_con",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket=Variable.get('gs_bucket'),
    filename="input_data/{{ds}}/data.json",
    dag=dag,
)

pgsl_to_gcs
