import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="p",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)


def print_exec_date(execution_date, **context):
    print(execution_date)


print_exec_date = PythonOperator(
    task_id="task_name",
    python_callable=print_exec_date,
    provide_context=True,
    dag=dag, )

sleep_1 = BashOperator(
    task_id="sleep_1",
    bash_command="sleep 1",
    dag=dag)

sleep_5 = BashOperator(
    task_id="sleep_5",
    bash_command="sleep 5",
    dag=dag)

sleep_10 = BashOperator(
    task_id="sleep_10",
    bash_command="sleep 10",
    dag=dag)

join = DummyOperator(
    task_id="join",
    dag=dag)

# Create a down stream dependency
print_exec_date >> [sleep_1, sleep_5, sleep_10] >> join
