import airflow.utils.dates
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3DeleteBucketOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator

sql_create_table: str = '''
    CREATE TABLE `log` (
    `id` int NOT NULL AUTO_INCREMENT,
    `ip` varchar(45) NOT NULL,
    `time` datetime NOT NULL,
    `first_redirect` varchar(45) NOT NULL,
    `present_redirect` varchar(100) NOT NULL,
    `method` varchar(45) NOT NULL,
    `request` varchar(45) NOT NULL,
    `status` int NOT NULL,
    `byte` int NOT NULL,
    `version` varchar(45) NOT NULL,
    `os` varchar(60) NOT NULL,
    `public` varchar(300) NOT NULL,
    `browser` varchar(45) NOT NULL,
    `location` varchar(45) NOT NULL,
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB AUTO_INCREMENT=83852 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
'''

# MySQL 연결 설정
mysql_conn_id = 'mysql_my_test'
mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
drop_t: str = f"DROP TABLE IF EXISTS dash.log;"
BUCKET_NAME = "sky-burket-test-injection"


with DAG(dag_id="storage_initalization_dag", 
        start_date=airflow.utils.dates.days_ago(3),
        schedule_interval=None,
        description="descriptrion mysql log parsing in generator log injection pipeline"
    ) as dag:
    
    
    drop_table_mysql = MySqlOperator(
        task_id="drop_log_table",
        mysql_conn_id="mysql_my_test",
        sql=drop_t,
        dag=dag
    )
    
    delete_s3_burket = S3DeleteBucketOperator(
        task_id='s3_bucket_dag_delete',
        force_delete=True,
        bucket_name=BUCKET_NAME,
        aws_conn_id="your_aws_connection_id",
        dag=dag
    )
    
    table_task_1 = MySqlOperator(
        task_id="create_log_table",
        mysql_conn_id="mysql_my_test",
        sql=sql_create_table,
        dag=dag
    )

    
    drop_table_mysql >> delete_s3_burket >> table_task_1