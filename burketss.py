import pymysql
import configparser
import csv
import boto3

parser = configparser.ConfigParser()
parser.read("pipeline.conf")

# mysql 
hostname = parser.get("mysql_config", "host")
port = parser.get("mysql_config", "port")
user = parser.get("mysql_config", "user")
password = parser.get("mysql_config", "password")
db = parser.get("mysql_config", "db")

# S3
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
burket_name = parser.get("aws_boto_credentials", "burket_name")
account_id = parser.get("aws_boto_credentials", "account_id")

conn = pymysql.connect(
    host=hostname,
    user=user,
    password=password,
    db=db,
    port=int(port)
)

qs = "SELECT * FROM data_init.log;"
if conn is None:
    print("Not is mysql connection")
else:
    m_curser = conn.cursor()
    m_curser.execute(qs)
    results = m_curser.fetchall()
    
    localfile_name = "order_extract_log.csv"
    with open(localfile_name, "w") as fp:
        csv_w = csv.writer(fp, delimiter=",")
        csv_w.writerows(results)
    fp.close()
    m_curser.close()
    conn.close()
    
    
s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key, 
    aws_secret_access_key=secret_key,
)
s3_file: str = localfile_name
s3.upload_file(localfile_name, burket_name, s3_file)