from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import csv
import time


def extract_mysql_data_to_s3() -> None:
    bucket_name = "sky-burket-test-injection"

    # MySQL configuration
    mysql_hook = MySqlHook(mysql_conn_id="mysql_my_test")
    qs = "SELECT * FROM dash.log;"
    results = mysql_hook.get_records(sql=qs)

    localfile_name = "order_extract_log.csv"
    with open(localfile_name, "w") as fp:
        csv_w = csv.writer(fp, delimiter=",")
        csv_w.writerows(results)
        fp.close()
    
    timestr = time.strftime("%Y%m%d-%H%M%S")

    out_file_name = f"access_log_{timestr}.csv"
    
        
    s3_hook = S3Hook(aws_conn_id="your_aws_connection_id")
    s3_hook.load_file(
        filename=localfile_name,
        key=out_file_name,
        bucket_name=bucket_name
    )


