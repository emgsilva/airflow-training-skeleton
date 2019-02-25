import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

from bigquery_get_data import BigQueryGetDataOperator

dag = DAG(
    dag_id='bqslack',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(2)
    }
)

bq_query = "select committer.name, count(*) as number \
        from [bigquery-public-data.github_repos.commits] \
        where date(committer.date) = '{{ ds }}' \
        group by committer.name \
        order by number desc \
        limit 5"

bq_fetch_data = BigQueryGetDataOperator(
    task_id='bq_fetch_data',
    sql=bq_query,
    dag=dag
)


def send_to_slack_func(execution_date, **context):
    operator = SlackAPIPostOperator(
        task_id='send_to_slack',
        text=str(execution_date + " >> " + context.get('ti').xcom_pull(key=None,
                                                                task_ids='bq_fetch_data')),
        token=Variable.get('slack_key'),
        # todo: should be passed into a variable from Airflow
        channel="general",
    )
    return operator.execute(context=context)


send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

bq_fetch_data >> send_to_slack
