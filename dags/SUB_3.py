from airflow import DAG
from airflow.operators import BashOperator, ExternalTaskSensor
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import time



default_args = {
    'owner': 'DMR',
    'depends_on_past': True,
    'start_date': datetime.now(),
    'email': ['sanddas@paypal.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Print job'

def my_display_function(phase):
    '''This is a function that will run within the DAG execution'''
    print ( "sleeping for {0}".format(phase))
    time.sleep(5)
sub_id=3
dag_ver='v1'

dag=DAG('SUB_{}_{}'.format(sub_id,dag_ver),default_args=default_args )
t1=PythonOperator(
            task_id='python_sub_{}_1'.format(sub_id),
            python_callable=my_display_function,
            op_kwargs={'phase': 'LOAD_DATA_START'},dag=dag
            )
t2=BashOperator(
            task_id='loaddata_{}'.format(sub_id),
            bash_command='sh /x/home/dm_hdp_batch/test/projects/steam_donkey/scripts/load_processing.sh ' ,dag=dag)
t3=PythonOperator(
            task_id='python_sub_{}_2'.format(sub_id),
            python_callable=my_display_function,
            op_kwargs={'phase': 'LOAD_DATA_END'},dag=dag)
t4=PythonOperator(
            task_id='python_sub_{}_3'.format(sub_id),
            python_callable=my_display_function,
            op_kwargs={'phase': 'APPLY_DATA_START'},dag=dag)
t5=BashOperator(
            task_id='applydata_{}'.format(sub_id),
            bash_command='sh /x/home/dm_hdp_batch/test/projects/steam_donkey/scripts/apply_processing.sh ',dag=dag)
t6=PythonOperator(
            task_id='python_sub_{}_4'.format(sub_id),
            python_callable=my_display_function,
            op_kwargs={'phase': 'APPLY_DATA_END'},dag=dag)
t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
t6.set_upstream(t5)
