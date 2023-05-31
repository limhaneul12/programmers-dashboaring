import re
import pandas as pd
import pymysql
import configparser
from typing import *

import re
import pandas as pd

parser = configparser.ConfigParser()
parser.read("pipeline.conf")

hostname = parser.get("mysql_config", "host")
port = parser.get("mysql_config", "port")
user = parser.get("mysql_config", "user")
password = parser.get("mysql_config", "password")
db = parser.get("mysql_config", "db")


def re_parsing_log_data(path: str = "access_log_.log") -> List[Tuple[str]]:
    pattern = re.compile(r'^([\d.]+) - - \[(.*?)\] (.*?) (.*?) "(.*?)" "(.*?)" (\d+) (\d+) (.*?) ([^)]+)\) (.*?) (\S+) "(.*?)"$')

    data = []
    with open(path, "r") as f:
        for line in f:
            match = pattern.match(line.strip())
            if match:
                groups = match.groups()
                groups = (groups[0], groups[1], groups[2], groups[3], 
                        groups[4], groups[5], groups[6], groups[7], groups[8], 
                        groups[9], groups[10], groups[11], groups[12])
                data.append(groups)
    return data


# 운영체제 정보 추출 함수
def extract_os(user_agent: str) -> None:
    os_pattern = r'Linux|Mac|Windows|iPhone'
    
    match = re.search(os_pattern, user_agent)
    if match:
        return match.group()
    else:
        return None


def log_data_saving() -> None:
    df = pd.DataFrame(re_parsing_log_data(), columns=["ip", "time", "first_redirect", 
                                                    "present_redirect", "method", "request", 
                                                    "status", "byte", "version", "os", 
                                                    "public", "browser", "location"])

    # 'os' 열에 운영체제 정보 추출하여 추가
    df['os'] = df['os'].apply(extract_os)
    df["time"] = pd.to_datetime(df["time"], format="%d/%b/%Y:%X", exact=False)
    df["time"] = pd.to_datetime(df["time"], format="%Y-%M-%d", exact=False)
    df.to_csv("order_extrect_log.csv")
    
    conn = pymysql.connect(
        host=hostname,
        user=user,
        password=password,
        db=db,
        port=int(port)
    )

    for index, row in df.iterrows():
        query = f"INSERT INTO log (ip, time, first_redirect, present_redirect, method, request, \
                                    status, byte, version, os, public, browser, location) VALUES \
            (\
            '{row['ip']}', '{row['time']}', '{row['first_redirect']}', \
            '{row['present_redirect']}', '{row['method']}', '{row['request']}', \
            '{row['status']}', '{row['byte']}', '{row['version']}', \
            '{row['os']}', '{row['public']}', '{row['browser']}', '{row['location']}'\
            )"
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
        

log_data_saving()