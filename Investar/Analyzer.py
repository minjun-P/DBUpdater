import pymysql
import pandas as pd
from datetime import datetime, timedelta
import re

class MarketDB:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host='localhost', user='root', passwd='Pmjshpmj78!', port=3306, db="investar", charset='utf8')
        self.codes = {}
        self.get_comp_info()

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    def get_comp_info(self):
        """company_info 테이블에서 읽어와서 codes에 저장"""
        sql = "SELECT * FROM company_info"
        krx = pd.read_sql(sql, self.conn)
        for idx in range(len(krx)):
            # 딕셔너리에 넣기 작업
            self.codes[krx['code'].values[idx]] = krx['company'].values[idx]

    def get_daily_price(self, code, start_date=None, end_date=None):
        """KRX 종목별 일별 시세를 데이터프레임 형태로 반환
            - code : KRX 종목코드('005930') 또는 상장기업명('삼성전자')
            - start_date : 조회 시작일('2020-01-01'), 미입력시 1년 전
            - end_date : 조회 종료일('2020-12-31'), 미입력시 오늘 날짜
        """
        if start_date is None:
            # 시작날짜 안 썻을 때 1년 전으로 초기화
            one_year_ago = datetime.today() - timedelta(days=365)
            start_date = one_year_ago.strftime('%Y-%m-%d')
            print("start_date is initialized to '{}'".format(start_date))
        else:
            # 날짜 형식 정규표현식으로 통일하기 
            start_list = re.split('\D+', start_date)
            if len(start_list[0])==8:
                # 20200808 형식일 때
                start_date = start_list[0]
                start_year = int(start_date[:4])
                start_month = int(start_date[4:6])
                start_day = int(start_date[6:])
            else:
                # 그 이외의 형식일 때 
                start_year = int(start_list[0])
                start_month = int(start_list[1])
                start_day = int(start_list[2])
            
            if start_year <1990 or start_year> 2200:
                print(f'ValueError: start_year({start_year:d}) is wrong.')
                return
            if start_month < 1 or start_month >12:
                print(f'ValueError: start_month({start_month:d}) is wrong.')
                return
            if start_day < 1 or start_day>31:
                print(f'ValueError: start_day({start_day:d}) is wrong.')
                return
            start_date = f"{start_year:04d}-{start_month:02d}-{start_day:02d}"

        if end_date is None:
            end_date = datetime.today().strftime('%Y-%m-%d')
            print("end_date is initialized to '{}'".format(end_date))
        else:
            # 날짜 형식 정규표현식으로 통일하기 
            end_list = re.split('\D+', end_date)
            if len(end_list[0])==8:
                # 20200808 형식일 때
                end_date = end_list[0]
                end_year = int(end_date[:4])
                end_month = int(end_date[4:6])
                end_day = int(end_date[6:])
            else:
                # 그 이외의 형식일 때 
                end_year = int(end_list[0])
                end_month = int(end_list[1])
                end_day = int(end_list[2])
            
            if end_year <1990 or end_year> 2200:
                print(f'ValueError: end_year({end_year:d}) is wrong.')
                return
            if end_month < 1 or end_month >12:
                print(f'ValueError: end_month({end_month:d}) is wrong.')
                return
            if end_day < 1 or end_day>31:
                print(f'ValueError: end_day({end_day:d}) is wrong.')
                return
            end_date = f"{end_year:04d}-{end_month:02d}-{end_day:02d}"

        codes_key = list(self.codes.keys())
        codes_values = list(self.codes.values())
        if code in codes_key:
            pass
        elif code in codes_values:
            idx = codes_values.index(code)
            code = codes_key[idx]
        else:
            print("ValueError: Code({}) doesn't exist.".format(code))

        sql = f"SELECT date, open, high, low, close, diff, volume FROM daily_price WHERE code = '{code}'"\
            f"and date >= '{start_date}' and date <= '{end_date}'"
        df = pd.read_sql(sql, self.conn)
        df.index = df['date']
        df = df[['open','high','low','close','diff','volume']]
        return df

if __name__ == '__main__':
    code = input('코드를 입력하시오')
    mk = MarketDB()
    result = mk.get_daily_price(code)
    print(result)