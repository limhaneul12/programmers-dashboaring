import time
import numpy
import datetime
import random
import gzip
import sys
from faker import Faker
import numpy as np


class FakeApacheLogGenerator:
    def __init__(self, 
                 num_lines: int = 1000, 
                 output_type: str = "CONSOLE", 
                 log_format: str = "ELF", 
                 file_prefix = None,
                 executor_date=None, 
                 sleep: float = 0.0):
        self.excutor_date = executor_date
        self.num_lines = num_lines
        self.output_type = output_type
        self.log_format = log_format
        self.file_prefix = file_prefix
        self.sleep = sleep
        self.faker = Faker()
        self.response = ["200", "404", "500", "301"]
        self.verb = ["GET", "POST", "DELETE", "PUT"]
        self.redirection_response = [
            "https://www.naver.com",
            "https://www.tistory.com",
            "https://www.youtube.com",
            "https://www.facebook.com",
            "https://www.instagram.com",
            "https://www.daum.com",
            "https://www.zoom.com",
            "https://www.linkedin.com",
            "https://www.google.com",
        ]
        self.resources = [
            "/list",
            "/wp-content",
            "/wp-admin",
            "/explore",
            "/search/tag/list",
            "/app/main/posts",
            "/posts/posts/explore",
            "/apps/cart.jsp?appID=",
        ]
        self.ualist = [
            self.faker.firefox,
            self.faker.chrome,
            self.faker.safari,
            self.faker.internet_explorer,
            self.faker.opera,
        ]
        self.cc = ['AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR', 'AS', 'AT', 'AU', 
                   'AW', 'AX', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BL', 
                   'BM', 'BN', 'BO', 'BQ', 'BR', 'BS', 'BT', 'BV', 'BW', 'BY', 'BZ', 'CA', 'CC', 
                   'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 
                   'CW', 'CX', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM', 'DO', 'DZ', 'EC', 'EE', 'EG', 
                   'EH', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FK', 'FM', 'FO', 'FR', 'GA', 'GB', 'GD', 
                   'GE', 'GF', 'GG', 'GH', 'GI', 'GL', 'GM', 'GN', 'GP', 'GQ', 'GR', 'GS', 'GT', 
                   'GU', 'GW', 'GY', 'HK', 'HM', 'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM', 
                   'IN', 'IO', 'IQ', 'IR', 'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 
                   'KI', 'KM', 'KN', 'KP', 'KR', 'KW', 'KY', 'KZ', 'LA', 'LB', 'LC', 'LK', 'LI', 
                   'LR', 'LS', 'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH', 
                   'MK', 'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW', 
                   'MX', 'MY', 'MZ', 'NC', 'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NU', 
                   'NZ', 'OM', 'PA', 'PE', 'PF', 'PG', 'PH', 'PK', 'PL', 'PM', 'PN', 'PR', 'PS', 
                   'PT', 'PW', 'PY', 'QA', 'RE', 'RO', 'RS', 'RU', 'RW', 'SA', 'SB', 'SC', 'SD', 
                   'SE', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'SS', 'ST', 
                   'SV', 'SX', 'SY', 'SZ', 'TC', 'TD', 'TF', 'TG', 'TH', 'TJ', 'TK', 'TL', 'TM', 
                   'TN', 'TO', 'TR', 'TT', 'TV', 'TW', 'TZ', 'UA', 'UG', 'UM', 'US', 'UY', 'UZ', 
                   'VA', 'VC', 'VE', 'VG', 'VI', 'VN', 'VU', 'WF', 'WS', 'YE', 'YT', 'ZA', 'ZM', 
                   'ZW']
        
    def normalization_guess_intraction(self, p: int) -> np.array:
        # 지수 분포로부터 값을 생성
        numbers = np.random.exponential(scale=1, size=p)

        # 생성된 값들을 합이 1이 되도록 정규화
        numbers /= np.sum(numbers)
        
        return numbers

    def generate_log(self):
        timestr = time.strftime("%Y%m%d-%H%M%S")
        otime = datetime.datetime.now()

        out_file_name = (
            "access_log_" + ".log"
            if not self.file_prefix
            else f"{self.file_prefix}_access_log_{timestr}.log"
        )
        
        if self.output_type == "LOG":
            f = open(out_file_name, "w")
        elif self.output_type == "GZ":
            f = gzip.open(out_file_name + ".gz", "w")
        elif self.output_type == "CONSOLE":
            f = sys.stdout
        else:
            raise ValueError("Invalid output type")

        flag = True
        while flag:
            if self.sleep:
                increment = datetime.timedelta(seconds=self.sleep)
            else:
                # executor_date부터 다음 실행 시간까지의 시간 간격을 지수 분포로 생성합니다.
                next_execution_date = otime + datetime.timedelta(days=3)
                num_intervals = 1440  # 1440개의 시간 간격을 생성합니다.

                # 지수 분포로부터 값을 생성합니다.
                intervals = self.normalization_guess_intraction(p=num_intervals)

                # 시간 간격을 계산합니다.
                total_interval = next_execution_date - otime
                increment = total_interval * intervals[self.num_lines % num_intervals]

            otime += increment
            ip = self.faker.ipv4()

            dt = otime.strftime("%d/%b/%Y:%H:%M:%S")
            tz = datetime.datetime.now().strftime("%z")
            vrb = numpy.random.choice(self.verb, p=self.normalization_guess_intraction(p=4))

            uri = random.choice(self.resources)
            if uri.find("apps") > 0:
                uri += str(random.randint(1000, 10000))

            resp = numpy.random.choice(self.response, p=[0.9, 0.04, 0.02, 0.04])
            byt = int(random.gauss(5000, 50))
            referer = self.faker.uri()
            user_agent = numpy.random.choice(self.ualist, p=self.normalization_guess_intraction(p=5))()
            code = random.choice(self.cc)
            
            start_redi = numpy.random.choice(self.redirection_response, p=self.normalization_guess_intraction(p=9))
            if self.log_format == "ELF":
                f.write(
                    '%s - - [%s %s] %s %s "%s" "%s HTTP/1.0" %s %s %s "%s"\n'
                    % (ip, dt, tz, start_redi, referer, vrb, uri, resp, byt, user_agent, code)
                )
            f.flush()

            self.num_lines -= 1
            flag = False if self.num_lines == 0 else True
            if self.sleep:
                time.sleep(self.sleep)


