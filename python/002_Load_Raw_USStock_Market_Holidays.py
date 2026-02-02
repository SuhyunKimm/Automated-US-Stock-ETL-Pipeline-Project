import finnhub
import pandas as pd
import numpy as np
import pyodbc
import time
import getLastLoadedDate
from datetime import timedelta, datetime

finnhub_client = finnhub.Client(api_key="")

with open(r"../logs/logs.txt", "a", encoding="utf-8") as f:
    for i in range(5) :
        try :
            finnhub_holidays = finnhub_client.market_holiday(exchange='US')
            break
        except Exception as e :
            f.wirte(f'Error : {e}. Retrying...{i}')
            time.sleep(5)

    df = pd.DataFrame(finnhub_holidays['data'])
    df.reset_index(drop=True, inplace=True)

    df[['Start_Time', 'End_Time']] = df['tradingHour'].str.split('-', expand=True)
    df['Status'] = np.where(df['tradingHour'].notna() & (df['tradingHour'] != ''), 'short day', 'closed')
    df = df[['atDate', 'Status', 'Start_Time', 'End_Time', 'eventName']]

    df.rename(columns = {
        "atDate" : "date",
        "Status" : "status",
        "Start_Time" : "startTime",
        "End_Time" : "endTime",
        "eventName" : "description"},
        inplace = True)

    df['startTime'] = df['startTime'].apply(lambda x: x if pd.notna(x) and x != '' else None)
    df['endTime'] = df['endTime'].apply(lambda x: x if pd.notna(x) and x != '' else None)

    sorted_df = df.sort_values(by='date')
    sorted_df.reset_index(drop=True, inplace=True)
    
    start_date = min(sorted_df['date'])
    last_loaded_date = getLastLoadedDate.getLastLoadedDate('us_market_holidays')
  
    if last_loaded_date != None and last_loaded_date != -1:
        start_date = last_loaded_date[0] + timedelta(days=1)

    start_date_str = start_date.strftime('%Y-%m-%d')

    filtered_df = sorted_df[sorted_df['date'] >= start_date_str]
    
    if filtered_df.empty :
        f.write('No new data to load.\n')
        exit()

    filtered_df = filtered_df.astype(object).where(pd.notnull(filtered_df), None)

    end_date = max(filtered_df['date'])
    end_date_fmt = datetime.strptime(end_date, '%Y-%m-%d').date()

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=;"
        "DATABASE=USStocks;"
        "UID=;"
        "PWD=;"
    )
    cursor = conn.cursor()

    f.write("Loading Us Market Holiday Data into SQL Server...")

    rows = list(filtered_df.itertuples(index=False, name=None))

    try :
        insert_query = "INSERT INTO raw.us_market_holidays ([date], [status], startTime, endTime, [description]) VALUES (?, ?, ?, ?, ?)"
        cursor.fast_executemany = True
        cursor.executemany(insert_query, rows)
        conn.commit()

        f.write(f"Successfully inserted {len(filtered_df)} records ({start_date} - {end_date}).") 

        update_query = "EXEC analytics.sp_upsert_etl_last_loaded @tableName=?, @lastLoadedDate=?"
        cursor.execute(update_query, ('us_market_holidays', end_date_fmt))
        conn.commit()
        f.write(f'Last loaded date for \'us_market_holidays\' has been updated.\n')

    except Exception as e :
        f.write(f"Error : {e}")

    finally:
        cursor.close()
        conn.close()
        f.close()