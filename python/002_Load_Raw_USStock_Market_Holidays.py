import finnhub
import pandas as pd
import numpy as np
import pyodbc
import time

finnhub_client = finnhub.Client(api_key="")

with open(r"C:\Automated-US-Stock-ETL-Pipeline-Project\logs\logs.txt", "a") as f:
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
        "atDate" : "Date",
        "eventName" : "Description"},
        inplace = True)

    df['Start_Time'] = df['Start_Time'].apply(lambda x: x if pd.notna(x) and x != '' else None)
    df['End_Time'] = df['End_Time'].apply(lambda x: x if pd.notna(x) and x != '' else None)

    sorted_df = df.sort_values(by='Date')

    sorted_df['Date'] = pd.to_datetime(sorted_df['Date'])

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=;"
        "DATABASE=USStocks;"
        "UID=;"
        "PWD=;"
    )
    cursor = conn.cursor()

    f.write("Loading Us Market Holiday Data into SQL Server...")

    rows = list(sorted_df.itertuples(index=False, name=None))

    try :
        insert_query = "INSERT INTO clean.us_market_holidays ([Date], [Status], Start_Time, End_Time, [Description]) VALUES (?, ?, ?, ?, ?)"
        cursor.fast_executemany = True
        cursor.executemany(insert_query, rows)
        conn.commit()

        f.write(f"Successfully inserted {len(sorted_df)} records.") 
        
    except Exception as e :
        f.write(f"Error : {e}")

    finally:
        cursor.close()
        conn.close()
        f.close()