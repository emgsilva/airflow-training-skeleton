import airflow
from airflow import DAG
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
        from bigquery-public-data.github_repos.commits \
        where date(committer.date) = {{ ds }} \
        group by committer.name \
        order by number asc \
        limit 5"

bq_fetch_data = BigQueryGetDataOperator(
    task_id='bq_fetch_data',
    sql=bq_query,
    dag=dag
)


def send_to_slack_func(**context):
    operator = SlackAPIPostOperator(
        task_id='send_to_slack',
        text=str(context['return_value']),
        # todo: should be passed into a variable
        token="xoxp-559854890739-559228586160-561116849751-2c717700dd7b7a197765ac21770c9c08",
        # todo: should be passed into a variable
        channel="General"
    )
    return operator.execute(context=context)


send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

bq_fetch_data >> send_to_slack
