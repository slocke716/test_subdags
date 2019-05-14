from task import sub_dag
from datetime import datetime
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.executors.kubernetes_executor import KubernetesExecutor


PARENT_DAG_NAME = 'parent_dag'
CHILD_DAG_NAME = 'child_dag'

main_dag = DAG(
  dag_id=PARENT_DAG_NAME,
  schedule_interval=None,
  start_date=datetime(2016, 1, 1)
)

sub_dag = SubDagOperator(
  subdag=sub_dag(PARENT_DAG_NAME, CHILD_DAG_NAME, main_dag.start_date,
                 main_dag.schedule_interval),
  task_id=CHILD_DAG_NAME,
  dag=main_dag,
  executor=KubernetesExecutor()
)
