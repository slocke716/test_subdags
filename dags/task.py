from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

DAG_ID = "task_1"
defaults = {
    "catchup": False,
    "depends_on_past": False,
    "mutex_keys": None,
    "start_date": datetime.now() - timedelta(minutes=10)
}


def create_dag(dag):
    start = DummyOperator(dag=dag, task_id="start")
    t1 = BashOperator(bash_command="sleep 1", task_id="task_1", dag=dag)
    t2 = BashOperator(bash_command="sleep 1", task_id="task_2", dag=dag)
    t3 = BashOperator(bash_command="sleep 1", task_id="task_3", dag=dag)

    start >> t1
    start >> t2
    start >> t3


# Dag is returned by a factory method
def sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    create_dag(dag)
    return dag

dag = DAG(
    dag_id=DAG_ID,
    default_args=defaults,
    schedule_interval=None,
)

create_dag(dag)
