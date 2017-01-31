from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import time


default_args = {
    'owner': 'DMR',
    'depends_on_past': True,
    'email': ['sanddas@paypal.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2017, 1, 30),
##    'queue': 'simba_queue'
}
def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Print job'

def my_display_function(phase):
    '''This is a function that will run within the DAG execution'''
    print ( "sleeping for {0}".format(phase))
    time.sleep(1)
    return
def conditionally_trigger(context, dag_run_obj):
    c_p =context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
           dag_run_obj.payload = {'message': context['params']['message']}
           print(dag_run_obj.payload)
           return dag_run_obj
pub_id=3
dag_ver='v2'
dag_id='PUB_{0}_{1}'.format(pub_id,dag_ver)
sub_id=3
sub_id_ver='v1'

dag=DAG(dag_id,  schedule_interval="30 5 * * *", default_args=default_args)
t1=PythonOperator(
            task_id='python_{}_1'.format(pub_id),
            python_callable=my_display_function,
            op_kwargs={'phase': 'BUILD_DELTA_START'},dag=dag
            )
t2=BashOperator(
            task_id='builddelta_{}'.format(pub_id),pool='simba_build_delta',
            bash_command='sh /x/home/dm_hdp_batch/test/projects/steam_donkey/scripts/delta_processing.sh ',dag=dag)
t3=PythonOperator(
            task_id='python_{}_2'.format(pub_id),
            python_callable=my_display_function,
            op_kwargs={'phase': 'BUILD_DELTA_END'},dag=dag)
t4=PythonOperator(
            task_id='python_{}_3'.format(pub_id),
            python_callable=my_display_function,
            op_kwargs={'phase': 'EXTRACT_DATA_START'},dag=dag)
t5=BashOperator(
            task_id='extractdata_{}'.format(pub_id),pool='simba_extract_data',
            bash_command='sh /x/home/dm_hdp_batch/test/projects/steam_donkey/scripts/export_processing.sh ',dag=dag)
t6=PythonOperator(
            task_id='python_{}_4'.format(pub_id),
            python_callable=my_display_function,
            op_kwargs={'phase': 'EXTRACT_DATA_END'},dag=dag)
t7=TriggerDagRunOperator(
    task_id='trigger_{}_1'.format(pub_id),
    trigger_dag_id="SUB_{}_{}".format(sub_id,sub_id_ver),
    python_callable=conditionally_trigger,
    params={'condition_param': True,
    'message': 'Hello World'},
    dag=dag)
t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
t6.set_upstream(t5)
t7.set_upstream(t6)

