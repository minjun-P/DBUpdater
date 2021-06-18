import pandas as pd
import pymysql
import calendar
import json
from datetime import datetime
import requests
from bs4 import BeautifulSoup as BS
from threading import Timer


class DBUpdater:
    def __init__(self):
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host='localhost', user='root', port=3307,
                                    passwd='Pmjshpmj78!', db='investar',
                                    charset='utf8')

        with self.conn.cursor() as curs:
            sql = """
            CREATE TABLE IF NOT EXISTS company_info (
                code VARCHAR(20),
                company VARCHAR(40),
                last_update DATE,
                PRIMARY KEY (code));
                """
            curs.execute(sql)
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price(
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date));
                """
            curs.execute(sql)
        self.conn.commit()
        # 여기까지 데이터베이스 연결 및 테이블 생성

        self.codes = dict()
        self.update_comp_info()
        # 코드가 쌓일 딕셔너리 생성 및 메서드 사용으로 업데이트

    def __del__(self):
        """소멸자: MariaDB 연결 해제"""
        self.conn.close()

    def read_krx_code(self):
        """KRX 로부터 상장법인목록 파일을 읽어와서 데이터프레임으로 변환"""
        # 다운로드 url 가져오는 방식! 기억할 것. post 방식일것이므로 개발자 도구에서 데이터 참고
        url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&pageIndex=1&currentPageSize=3000' \
              '&orderMode=3&orderStat=D&searchType=13&fiscalYearEnd=all&location&all '
        krx = pd.read_html(url, header=0)[0]
        # 파일 가져와서 테이블 읽기 - excel 파일도 읽어주는 pandas ㄷㄷㄷ
        krx = krx[['종목코드', '회사명']]
        # 중요한 컬럼 두개만 남기기 및 이름 바꾸기, 코드 전처리
        krx = krx.rename(columns={'종목코드': 'code', '회사명': 'company'})
        krx.code = krx.code.map('{:>06d}'.format)

        return krx
        # 코드와 회사명 2개의 열을 갖고 있는 데이터프레임 리턴

    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM company_info"
        df = pd.read_sql(sql, self.conn)
        for idx in range(len(df)):
            # 이미 만들어 놓은 codes 딕셔너리에 값 넣기 .values 메소드로 array 화 한 뒤, 인덱싱
            self.codes[df['code'].values[idx]] = df['company'].values[idx]
            # 이미 테이블에 들어가 있는 코드와 회사명을 딕셔너리에 매치해 채워주기

        with self.conn.cursor() as curs:
            # 마지막 업데이트 날짜 뽑아오기
            sql = "SELECT max(last_update) FROM company_info"
            curs.execute(sql)
            rs = curs.fetchone()
            today = datetime.today().strftime('%Y-%m-%d')

            # if문 -> 1.테이블에 값이 없거나(처음 데이터를 넣는 경우) 2. 최근 업데이트 날짜가 오늘보다 오래된 경우에 한해 코드 실행

            if rs[0] is None or rs[0].strftime('%Y-%m-%d') < today:
                # 메소드 활용 - 엑셀 다운로드 후 목록 뽑기(코드,회사명)
                krx = self.read_krx_code()
                for idx in range(len(krx)):
                    # 엑셀에서 코드, 회사명 뽑은 후 임시 변수에 저장
                    code = krx.code.values[idx]
                    company = krx.company.values[idx]

                    # sql 문 실행해서 임시 변수에 담은 데이터를 테이블에 업데이트
                    sql = "REPLACE INTO company_info (code, company," \
                          "last_update) VALUES ('{}','{}','{}')".format(code, company, today)
                    curs.execute(sql)
                    # 딕셔너리에 해당 회사와 종목 코드 매치해 넣기
                    self.codes[code] = company
                    nowtime = datetime.now().strftime('%Y-%m-%d %H:%M')
                    # 진행상황 확인 겸, 구문 출력
                    print("[{0}] {1} REPLACE INTO company_info VALUES ({2}, {3}, {4})".format(nowtime, idx, code,
                                                                                              company, today))
                self.conn.commit()
                print('')

    def read_naver(self, code, company, pages_to_fetch):
        """네이버 금융에서 주식 시세를 읽어서 데이터프레임으로 반환"""

        # 시세 페이지 html 요청 및 일차적으로 필요한 요소 값 가져오기
        url = 'https://finance.naver.com/item/sise_day.nhn?code={}'.format(code)
        headers = {
            'User-Agent':
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 "
                "Safari/537.36"}
        with requests.get(url, headers=headers) as doc:
            # 오류 방지
            if doc is None:
                return None

            html = BS(doc.content, 'lxml')
            pgrr = html.select_one('td.pgRR')

            # 오류 방지
            if pgrr is None:
                return None

            lastpage = pgrr.a['href'].split('=')[-1]

        df = pd.DataFrame()

        if pages_to_fetch == 'all':
            pages = int(lastpage)
        else:
            pages = min(int(lastpage), pages_to_fetch)
        # 1페이지부터 원하는 페이지까지 데이터 크롤링해서 데이터프레임에 담기
        for page in range(1, pages + 1):
            pg_url = '{}&page={}'.format(url, page)
            doc = requests.get(pg_url, headers=headers)
            df = df.append(pd.read_html(doc.content, header=0)[0])
            tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
            # 파이참에서만 '\r' 실행이 안되는 버그 -> 억지로 구현함. 굳이 안해도 되는 거긴 해
            print('\r' + '[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page,
                                                                                        pages), end='')
        # 한줄 넘어가게 해주기
        print('')

        # 뽑아온 데이터프레임 후처리하기 (컬럼명 변경, na 삭제, 타입변경, 컬럼 순서 변경)
        df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff', '시가': 'open', '고가': 'high', '저가': 'low',
                                '거래량': 'volume'})
        df = df.dropna()
        # map 의 활용! 기억해둘 것! => dropna를 한 다음에 해야 오류가 없음.
        df['date'] = df['date'].map(lambda x: x.replace('.', '-'))
        df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[
            ['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)
        df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]

        return df

    def replace_into_db(self, df, num, code, company):
        """네이버 금융에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:
            # 데이터프레임에 넣어놨던 시세 정보를 이터레이터 튜플로 만들어 반복문에 활용 -> 순차적으로 집어넣기
            for r in df.itertuples():
                sql = f"REPLACE INTO daily_price VALUES ('{code}','{r.date}',{r.open}," \
                      f"{r.high},{r.low},{r.close},{r.diff},{r.volume})"
                curs.execute(sql)
            self.conn.commit()

            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] #{num + 1:04d} {company} ({code}) : {len(df)}"
                  f"rows > REPLACE INTO daily_price [OK]")

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes):
            df = self.read_naver(code, self.codes[code], pages_to_fetch)
            if df is None:
                continue
            self.replace_into_db(df, idx, code, self.codes[code])

    def execute_daily(self):
        """실행 즉시 및 매일 오후 다섯시에 daily_price 테이블 업데이트"""
        self.update_comp_info()
        try:
            with open('config.json', 'r') as in_file:
                # 디스크에 있는 json 파일에 담긴 데이터를 config 변수에 저장
                config = json.load(in_file)
                pages_to_fetch = config['pages_to_fetch']
        except FileNotFoundError:
            with open('config.json', 'w') as out_file:
                pages_to_fetch = 100
                config = {'pages_to_fetch': 1}
                # config 변수에 담은 dict 형식의 데이터를 json 화 해서 파일에 담고 저장
                json.dump(config, out_file)
        self.update_daily_price(pages_to_fetch)

        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]

        # 1년의 마지막 날인 경우
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year + 1, month=1, day=1, hour=17, minute=0, second=0)

        # 한 달의 마지막 날인 경우
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month + 1, day=1, hour=17, minute=0, second=0)

        # 그 나머지
        else:
            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0, second=0)

        # 다음 업데이트까지 남은 초 구하기 - 타이머에 활용할 것
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds

        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()


if __name__ == '__main__':
    dbu = DBUpdater()
    dbu.execute_daily()
