import os, sys
import argparse
import datetime

import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from urllib import request as req
from urllib.request import urlopen
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Stock Data Crawling with company names that i want to collect in KRX.')
parser.add_argument('--company_names', type=str, nargs='+', help='Company names in KRX')
parser.add_argument('--num_years', type=int, default=2)
parser.add_argument('--save_dir', type=str)

args = parser.parse_args()

if __name__ == '__main__':
    krx_html_path = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    krx_list = pd.read_html(krx_html_path)
    krx_list[0]['종목코드'] = krx_list[0]['종목코드'].map(lambda x: '{:06d}'.format(x))
    krx_df = krx_list[0]
    krx_df = krx_df.sort_values(by='종목코드', ascending=False)

    company_list = args.company_names
    filtered_krx_df = krx_df.query(f'회사명 in {company_list}')

    codes = []
    new_company_list = []
    for i, row in filtered_krx_df.iterrows():
        codes.append(row['종목코드'])
        new_company_list.append(row['회사명'])

    print(f"---------- Start Crawling with Stock Codes {new_company_list} ----------")
    print("\n".join(company_list))

    start_date = datetime.date.today()
    year_3 = datetime.timedelta(days=int(365*args.num_years))

    end_date = start_date - year_3
    end_date = end_date.strftime("%Y-%m-%d")
    start_date = start_date.strftime("%Y-%m-%d")

    print(f"\tStart Date(=Today) : {start_date}")
    print(f"\tEnd Date : {end_date}")

    os.makedirs(args.save_dir, mode=0o777, exist_ok=True)
    for code in tqdm(codes):
        url = f"https://finance.naver.com/item/sise_day.nhn?code={code}&page=1"
        headers = ("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36")

        opener = req.build_opener()
        opener.addheaders = [headers]
        with opener.open(url) as response:
            doc = BeautifulSoup(response, 'lxml')
            pgrr = doc.find('td', class_='pgRR')
            # print(pgrr.a['href'])
            last_page = pgrr.a['href'].split("=")[-1]

        df = pd.DataFrame()
        sise_url = f"https://finance.naver.com/item/sise_day.nhn?code={code}" # 셀트리온 종목코드 : 068270

        for page in range(1, int(last_page)+1):
            page_url = '{}&page={}'.format(sise_url, page)
            response = opener.open(page_url)
            _df = pd.read_html(response, header=0)[0].dropna().drop(columns=['전일비'])

            _df = _df.rename(columns={'날짜': 'Date', '시가': 'Open', '고가': 'High', '저가': 'Low', '종가': 'Close', '거래량': 'Volume'})
            _df.index = pd.to_datetime(_df['Date'])
            _df = _df[['Open', 'High', 'Low', 'Close', 'Volume']]

            df = df.append(_df)

            if _df.index.max().strftime('%Y-%m-%d') < end_date:
                df = df[df.index >= end_date]
                break

        save_path = os.path.join(args.save_dir, f"{code}_{start_date}_{end_date}.csv")
        df.to_csv(save_path)
        print(f"\t Save into {save_path}")

    print("Complete.")