from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'yourname',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 26),
    'email': ['youremail'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('sequencing_analysis', default_args=default_args, schedule_interval=timedelta(hours=24))

t1 = SSHOperator(task_id='down_load_data', ssh_conn_id="ssh_spark_cluster_2", command="python3 /home/ubuntu/download_data.py", dag=dag)
t2 = SSHOperator(task_id='divide_large_file', ssh_conn_id="ssh_spark_cluster_2", command="python3 /home/ubuntu/divide_fasta.py", dag=dag)
t3 = SSHOperator(task_id='decompress_file', ssh_conn_id="ssh_spark_cluster_2", command="python3 /home/ubuntu/decompress_file.py", dag=dag)
t4 = SSHOperator(task_id='WGS_processing', ssh_conn_id="ssh_spark_cluster_2", command="bash /home/ubuntu/alignment.py", dag=dag)
t5 = SSHOperator(task_id='annotation', ssh_conn_id="ssh_spark_cluster_2", command="python3 /home/ubuntu/annotation.py", dag=dag)

