import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable


from parsing import log_data_saving
from burketss import extract_mysql_data_to_s3
from apachefakeloggen import FakeApacheLogGenerator



def generate_fake_logs(execution_date: str) -> None:
    num_lines = int(Variable.get("num_lines", default_var=1000))
    output_type = Variable.get("output_type", default_var="LOG")
    log_format = Variable.get("log_format", default_var="ELF")
    file_prefix = Variable.get("file_prefix", default_var=None)
    sleep = float(Variable.get("sleep", default_var=0.0))

    log_generator = FakeApacheLogGenerator(
        num_lines=num_lines,
        output_type=output_type,
        log_format=log_format,
        file_prefix=file_prefix,
        executor_date=execution_date,
        sleep=sleep,
    )
    log_generator.generate_log()


# MySQL 연결 설정
mysql_conn_id = 'mysql_my_test'
mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
drop_t: str = f"DROP TABLE IF EXISTS dash.log;"
BUCKET_NAME = "sky-burket-test-injection"


default_args = {
    'start_date': datetime.datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(dag_id="log_saving_interaction_dag",
         default_args=default_args,
         schedule_interval='0 0 * * *',
         description="Description: MySQL log parsing in generator log injection pipeline") as dag:

    def extract_execute_date(**kwargs):
        execution_date = kwargs['execution_date'].isoformat()
        return execution_date

    log_start = PythonOperator(
        task_id="log_start",
        python_callable=generate_fake_logs,
        op_kwargs={'execution_date': '{{ execution_date }}'},
        dag=dag
    )

    my_task = PythonOperator(
        task_id='my_task',
        python_callable=extract_execute_date,
        provide_context=True,
        dag=dag
    )
    
    log_data_extract = PythonOperator(
        task_id="log_extract",
        python_callable=log_data_saving,
        dag=dag
    )
    
    create_bucket = S3CreateBucketOperator(
        task_id='s3_bucket_dag_create',
        bucket_name=BUCKET_NAME,
        aws_conn_id='your_aws_connection_id',
        dag=dag
    )

    log_saving = PythonOperator(
        task_id="log_saving",
        python_callable=extract_mysql_data_to_s3,
        dag=dag
    )


    my_task >> log_start >> log_data_extract >> create_bucket >> log_saving

