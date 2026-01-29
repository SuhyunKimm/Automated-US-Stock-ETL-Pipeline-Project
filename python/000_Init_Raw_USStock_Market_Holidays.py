import pandas as pd
import numpy as np
import pyodbc
import finnhub
import time

df = pd.read_csv('../resources/nyse_holidays_latest.csv')
df = df[df['date'] >= '2021-01-01']
df = df[['date', 'status', 'start_time', 'end_time', 'holiday_name']]

df.rename(columns = {
    "date" : "Date",
    "status" : "Status",
    "start_time" : "Start_Time",
    "end_time" : "End_Time",
    "holiday_name" : "Description"},
    inplace = True)

df['Date'] = pd.to_datetime(df['Date'])

df['Start_Time'] = df['Start_Time'].replace(np.nan, None)
df['End_Time'] = df['End_Time'].replace(np.nan, None)

start_date = min(df['Date'])
end_date = max(df['Date'])

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=;"
        "DATABASE=USStocks;"
        "UID=;"
        "PWD=;"
)
cursor = conn.cursor()

try :
    insert_query = "INSERT INTO clean.us_market_holidays ([Date], [Status], Start_Time, End_Time, [Description]) VALUES (?, ?, ?, ?, ?)"
    
    print("Loading Us Market Holiday Data into SQL Server...")
    
    rows = list(df.itertuples(index=False, name=None))
    cursor.executemany(insert_query, rows)
    conn.commit()
    print(f"Successfully inserted {len(df)} records ({start_date} - {end_date}).") 

except Exception as e :
    print(f"Error : {e}")

finnhub_client = finnhub.Client(api_key="")

for i in range(5) :
    try :
        finnhub_holidays = finnhub_client.market_holiday(exchange='US')
        break
    except Exception as e :
        print(f'Error : {e}. Retrying...{i}')
        time.sleep(5)
    
df = pd.DataFrame(finnhub_holidays['data'])
df.reset_index(drop=True, inplace=True)

df[['Start_Time', 'End_Time']] = df['tradingHour'].str.split('-', expand=True)
df['Status'] = np.where(df['tradingHour'].notna() & (df['tradingHour'] != ''), 'short day', 'closed')
df = df[['atDate', 'Status', 'Start_Time', 'End_Time', 'eventName']]

df.rename(columns = {
    "atDate" : "Date",
    "eventName" : "Description"},
    inplace = True)

df['Start_Time'] = df['Start_Time'].apply(lambda x: x if pd.notna(x) and x != '' else None)
df['End_Time'] = df['End_Time'].apply(lambda x: x if pd.notna(x) and x != '' else None)

sorted_df = df.sort_values(by='Date')

sorted_df['Date'] = pd.to_datetime(sorted_df['Date'])
sorted_df = sorted_df[sorted_df['Date'] > end_date]

start_date2 = min(sorted_df['Date'])
end_date2 = max(sorted_df['Date'])

try :
    insert_query = "INSERT INTO clean.us_market_holidays ([Date], [Status], Start_Time, End_Time, [Description]) VALUES (?, ?, ?, ?, ?)"
    
    print("Loading Us Market Holiday Data into SQL Server...")
    
    rows = list(sorted_df.itertuples(index=False, name=None))
    cursor.fast_executemany = True
    cursor.executemany(insert_query, rows)
    conn.commit()
    print(f"Successfully inserted {len(sorted_df)} records ({start_date2} - {end_date2}).") 

except Exception as e :
    print(f"Error : {e}")

finally:
    cursor.close()
    conn.close()