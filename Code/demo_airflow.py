from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow_master',
    'depends_on_past': False,
    'start_date': datetime(2021,12,23, 10 ,0), #days_ago(2), #datetime(2021,11,23)
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}
    
with DAG('demo_airflow', default_args=default_args, schedule_interval=None) as dag:

    Start = DummyOperator(task_id = 'Start')
       
    sql_command_1 = Variable.get("sql_command_1")
    sp_ctrl_calendar = PostgresOperator(
        task_id = 'sp_ctrl_calendar',
        sql = sql_command_1,
        postgres_conn_id = 'redshift_conn',
        autocommit = True)
    
    End = DummyOperator(task_id = 'End')
       
Start >> sp_ctrl_calendar >> End
