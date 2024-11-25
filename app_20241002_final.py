
import requests
from datetime import datetime
import time
import pandas as pd
import pymssql

class BinanceDataFetcher:
    COLUMNS = ['open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'close_time', 'quote_av', 'trades', 
               'tb_base_av', 'tb_quote_av', 'ignore']
    URL = 'https://api.binance.com/api/v3/klines'

    def __init__(self, start_date, end_date, symbol):
        self.start_date = start_date
        self.end_date = end_date
        self.symbol = symbol


    def fetch_data(self):
        data = []
        start = int(time.mktime(datetime.strptime(self.start_date + ' 00:00', '%Y-%m-%d %H:%M').timetuple())) * 1000
        end = int(time.mktime(datetime.strptime(self.end_date +' 23:59', '%Y-%m-%d %H:%M').timetuple())) * 1000
        params = {
            'symbol': self.symbol,
            'interval': '1d',
            'limit': 1000,
            'startTime': start,
            'endTime': end
        }
        while start < end:
            print("=====start======")
            params['startTime'] = start
            result = requests.get(self.URL, params = params)
            js = result.json()
            #print(js)
            if not js:
                break
            data.extend(js)
            start = js[-1][0] + 60000
            time.sleep(3)
            print(data)

        if not data:
            print('해당 기간에 일치하는 데이터가 없습니다.')
            return -1
        df = pd.DataFrame(data)
        df.columns = self.COLUMNS
        df['open_time'] = df.apply(lambda x:datetime.fromtimestamp(x['open_time'] // 1000), axis=1)
        df = df.drop(columns = ['close_time', 'ignore'])
        df['symbol'] = self.symbol
        df.loc[:, 'open_price':'tb_quote_av'] = df.loc[:, 'open_price':'tb_quote_av'].astype(float)
        df['trades'] = df['trades'].astype(int)

        self.data = df

    def delete_db(self):
        conn =  pymssql.connect(server='127.0.0.1', database='coin_db', user='test', password='test', charset='EUC-KR')
        cursor = conn.cursor()
        query = "delete from coin_db.dbo.price where open_time >='" + str(self.start_date) +" 00:00:00' and open_time <= '" + str(self.end_date) +" 23:00:00' and symbol = '"+self.symbol+"';"
        print(query)
        cursor.execute(query)
        conn.commit()
        cursor.close()

    def insert_db(self):
        conn =  pymssql.connect(server='127.0.0.1', database='coin_db', user='test', password='test', charset='EUC-KR')
        cursor = conn.cursor()
        for index, row in self.data.iterrows():
            query = "INSERT INTO coin_db.dbo.price (open_time, open_price, high_price, low_price, close_price, volume,quote_av, trades, tb_base_av,\
                    tb_quote_av, symbol, create_time, update_time) values('" + str(row.open_time) +"','" + str(row.open_price) +"','" + str(row.high_price) +"','" + str(row.low_price)\
                        +"','" + str(row.close_price) +"','" + str(row.volume) +"','" + str(row.quote_av) +"','" + str(row.trades) +"','" + str(row.tb_base_av)\
                            +"','" + str(row.tb_quote_av) +"','" + str(row.symbol) +"','" + str(datetime.now()) +"','" + str(datetime.now()) +"');"
            print(query)
            cursor.execute(query)
            
            conn.commit()
        cursor.close()

# Usage


start_date = '2018-01-01'
end_date = '2024-09-22'
date_range = pd.date_range(start=start_date, end=end_date)
date_list = date_range.strftime('%Y-%m-%d').tolist()

print(date_list)
chunk_size = 30
new_list = [date_list[i:i + chunk_size] for i in range(0, len(date_list), chunk_size)]

print(new_list)

#for i in new_list:
#    print(min(i), max(i))
def process(start,end):
    fetcher = BinanceDataFetcher(start, end, 'BTCUSDT')
    fetcher.fetch_data()
    fetcher.delete_db()
    fetcher.insert_db()

from concurrent.futures import ThreadPoolExecutor

if __name__ == "__main__":#

    
    
    with ThreadPoolExecutor(max_workers=len(new_list)) as executor:
       
       futures = [executor.submit(process, min(i),max(i)) for i in new_list]
       

    #executor = ThreadPoolExecutor(len(new_list))
    #executor.submit()

#for i in range(0, len(date_list)-1):
#    print("====here====")
#    if i == len(date_list):
#        break
#    fetcher = BinanceDataFetcher(date_list[i], date_list[i+1], 'BTCUSDT')  # Replace with your symbol
#    fetcher.fetch_data()

#    fetcher.delete_db()
    #fetcher.insert_db()
    

# 각 연도별 비트코인의 최대 최소값. (각연도별)    
#for i in date_list:
#    conn =  pymssql.connect(server='127.0.0.1', database='coin_db', user='test', password='test', charset='EUC-KR')
#    cursor = conn.cursor()
#    sql = "select max(high_price), min(low-price) from coin_db.dbo.price where YEAR(open_time) = " + i +";"
    #print(sql)
#    cursor.execute(sql)
#    conn.commit()
    #print("===commit")
#    cursor.close()
   #  select YEAR(open_time), max(high_price), min(low_price) from coin_db.dbo.price group by YEAR(open_time) order by YEAR(open_time)
#select max(high_price), min(low_price) from coin_db.dbo.price where YEAR(open_time) = 2019














