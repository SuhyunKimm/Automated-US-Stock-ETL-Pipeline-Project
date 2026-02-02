import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime

df = pd.read_csv('../resources/nyse_holidays_latest.csv')
df = df[df['date'] >= '2021-01-01']
df = df[['date', 'status', 'start_time', 'end_time', 'holiday_name']]
with open(r"../logs/logs.txt", "a", encoding="utf-8") as f:
    df.rename(columns = {
        "date" : "date",
        "status" : "status",
        "start_time" : "startTime",
        "end_time" : "endTime",
        "holiday_name" : "description"},
        inplace = True)
    
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

    start_date = min(df['date'])
    end_date = max(df['date'])

    end_date_fmt = datetime.strptime(end_date, '%Y-%m-%d').date()
 
    conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=;"
            "DATABASE=USStocks;"
            "UID=;"
            "PWD=;"
    )
    cursor = conn.cursor()

    try :
        insert_query = "INSERT INTO raw.us_market_holidays ([date], [status], startTime, endTime, [description]) VALUES (?, ?, ?, ?, ?)"
        
        f.write("Loading Us Market Holiday Data into SQL Server...")
        
        df = df.astype(object).where(pd.notnull(df), None)
        rows = list(df.itertuples(index=False, name=None))
        cursor.executemany(insert_query, rows)
        conn.commit()
        f.write(f"Successfully inserted {len(df)} records ({start_date} - {end_date}).") 

        update_query = "EXEC analytics.sp_upsert_etl_last_loaded @tableName=?, @lastLoadedDate=?"
        cursor.execute(update_query, ('us_market_holidays', end_date_fmt))
        conn.commit()
        f.write(f'Last loaded date for \'us_market_holidays\' has been updated.\n')

    except Exception as e :
        print(f"Error : {e}")

    finally:
        cursor.close()
        conn.close()
        f.close()