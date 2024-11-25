from datetime import datetime, timedelta
import pymssql
import pandas as pd
def get_previous_5_days(start_date, days_to_calculate=10):
    """
    start_date: 기준 날짜 (예: '2024-01-06')
    days_to_calculate: 날짜 리스트를 구할 기간 (몇일동안 동적으로 리스트를 구할지)
    """
    # 시작 날짜를 datetime 객체로 변환
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    
    # 각 날짜별로 5일 전 날짜 리스트를 구함
    for i in range(days_to_calculate): # 0 1 2 3 4 5 6 
        current_date = start_date + timedelta(days=i)  # 기준 날짜에서 i일 더함
        five_days_list = [(current_date - timedelta(days=j)).strftime('%Y-%m-%d') for j in range(1, 6)]
        print(f"기준 날짜: {current_date.strftime('%Y-%m-%d')} -> 5일 전 날짜들: {five_days_list}")

# 예시 실행: 2024-01-06을 시작으로 10일 동안 날짜 리스트 구하기
#get_previous_5_days('2024-01-01', 20)
"""get_previous_5_days 함수는 주어진 날짜부터 동적으로 날짜를 하루씩 증가시키며, 해당 날짜에서 5일 전까지의 날짜를 리스트로 반환합니다.
timedelta(days=j): 날짜를 조정하기 위해 사용되며, j값이 1부터 5까지로 설정되어 1일에서 5일 전의 날짜를 계산합니다.
strftime('%Y-%m-%d'): 날짜를 문자열 형식으로 출력하기 위해 사용됩니다.
for i in range(days_to_calculate): 기준 날짜에서 매일 1일씩 증가시키면서 동적으로 계산합니다."""


def insert_db(self):
    conn =  pymssql.connect(server='127.0.0.1', database='coin_db', user='test', password='test', charset='EUC-KR')
    cursor = conn.cursor()
    for index, row in self.data.iterrows():
        query = "INSERT INTO coin_db.dbo.price (open_time, open_price,  volume,quote_av, \
                symbol) values('" + str(row.open_time) +"','" + str(row.open_price) +"','" + str(row.high_price) +"','" + str(row.low_price)\
                    +"','" + str(row.close_price) +"','" + str(row.volume) +"','" + str(row.quote_av) +"','" + str(row.trades) +"','" + str(row.tb_base_av)\
                        +"','" + str(row.tb_quote_av) +"','" + str(row.symbol) +"','" + str(datetime.now()) +"','" + str(datetime.now()) +"');"
        print(query)
        cursor.execute(query)
        
        conn.commit()
    cursor.close()

#사용하는것
def get_previous_days(start_date, days_to_calculate=1, days_range=5):
    """
    start_date: 기준 날짜 (예: '2024-01-06')
    days_to_calculate: 날짜 리스트를 구할 기간 (며칠 동안 동적으로 리스트를 구할지)
    days_range: 각 기준 날짜에서 몇 일 전까지 날짜를 리스트에 넣을지
    """
    # 시작 날짜를 datetime 객체로 변환
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    
    # 각 날짜별로 days_range만큼의 날짜 리스트를 구함
    for i in range(days_to_calculate):
        current_date = start_date + timedelta(days=i)  # 기준 날짜에서 i일 더함
        previous_days_list = [(current_date - timedelta(days=j)).strftime('%Y-%m-%d') for j in range(1, days_range+1)]
        print(f"기준 날짜: {current_date.strftime('%Y-%m-%d')} -> {days_range}일 전 날짜들: {previous_days_list}")

        
        conn =  pymssql.connect(server='127.0.0.1', database='coin_db', user='test', password='test', charset='EUC-KR')



        cursor = conn.cursor()

        # make where condition
        condition = ''
        for date in previous_days_list:
            condition += '\'' + str(date) + ' 09:00:00\','
            
        condition = condition[:-1]
        
        #print("zzzz")
        #print(condition)
        sql = "SELECT open_time, open_price, high_price, low_price, close_price, volume, symbol FROM price WHERE open_time in (" + condition + ")"
        cursor.execute(sql)
        rows = cursor.fetchall()
        #print(type(rows))
        #for i in rows:
        #    print(type(i))




        df = pd.DataFrame(rows, columns=["open_time", "open_price", "high_price", "low_price", "close_price", "volume", "symbol"])
        
        #print("데이터 원본....")
        #print(df)
        #print("----------------------------------")
        
        sql = "SELECT open_time, open_price, high_price, low_price, close_price, volume, symbol FROM price WHERE open_time = '" + str(datetime.strftime(start_date, '%Y-%m-%d')) + " 09:00:00'"
        #print(sql)
        cursor.execute(sql)

        row = cursor.fetchall()

        df2 = pd.DataFrame(row, columns=["open_time", "open_price", "high_price", "low_price", "close_price", "volume", "symbol"])
        
        avg_5 = sum(df['close_price'])/5
        df2['avg_5'] = avg_5
        print("데이터 가공후....")
        print(df2)
        

        #1월 6일자 쿼리를 날리고. 그 데이터를 가져온 후 그 끝에 avg_5를 붙여주고 insert 하면 끝.


        #for date in previous_days_list:
        #    five_day_lind = 0
        #    query = "SELECT open_time, open_price, high_price, low_price, close_price, volume, symbol FROM coin WHERE date_column = ?", (date)
        #    print(query)
        #    rows = cursor.fetchall()
        #    cursor.execute(query)
        #    rows = cursor.fetchall()
        #    print(f"기준 날짜: {current_date.strftime('%Y-%m-%d')} -> {date}에 해당하는 데이터: {rows}")
#

# 데이터베이스 연결 종료
        conn.close()

        return df2


#사용하는것
def upsert_price_data(df, table_name, pk_column="open_time"):
    # pymssql을 사용하여 MSSQL 데이터베이스 연결
    with pymssql.connect(server='127.0.0.1', database='coin_db', user='test', password='test', charset='EUC-KR') as conn:
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            # Delete query
            delete_sql = f"DELETE FROM {table_name} WHERE {pk_column} = %s"
            cursor.execute(delete_sql, (row[pk_column],))
        
        # 데이터 삽입 쿼리 작성
        insert_sql = f"""
            INSERT INTO {table_name} (open_time, open_price, high_price, low_price, close_price, volume, symbol, avg_5)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 데이터 삽입
        for _, row in df.iterrows():
            cursor.execute(insert_sql, (
                row["open_time"], row["open_price"], row["high_price"], 
                row["low_price"], row["close_price"], row["volume"], 
                row["symbol"], row["avg_5"]
            ))
        
        # 변경사항 커밋
        conn.commit()



from datetime import datetime, timedelta

# 시작 날짜와 오늘 날짜 설정
start_date = datetime(2024, 1, 1)
end_date = datetime.now()

# 시작 날짜부터 오늘 날짜까지 반복
current_date = start_date
while current_date <= end_date:
    target_date = str(current_date.strftime("%Y-%m-%d"))
    
    # 함수 호출 예시
    df = get_previous_days(target_date)

    # 데이터 삽입 함수 실행
    upsert_price_data(df, "price_test")

    current_date += timedelta(days=1)


#get_previous_days(2018-1-1, days_to_calculate=2, days_range=5)

    

"""days_range: 이 변수는 각 기준 날짜에서 며칠 전까지의 날짜를 리스트에 포함할지 조절합니다. 예를 들어, days_range=10으로 설정하면 기준 날짜에서 10일 전까지의 날짜가 리스트에 들어갑니다.

for j in range(1, days_range+1): days_range 값에 따라 날짜 리스트를 생성합니다. j는 1부터 시작하여 days_range 값만큼 반복합니다.

days_to_calculate: 기준 날짜부터 몇 번의 날짜 계산을 할지 결정합니다."""


['2024-01-01','2024-01-02','2024-01-03','2024-01-04','2021-01-05'] # << 리스트
#-> '2024-01-01','2024-01-02','2024-01-03','2024-01-04','2021-01-05' <<문자열 

"select avg(close_price) from price where open_time in ('2024-01-01','2024-01-02','2024-01-03','2024-01-04','2021-01-05')"



#1. 