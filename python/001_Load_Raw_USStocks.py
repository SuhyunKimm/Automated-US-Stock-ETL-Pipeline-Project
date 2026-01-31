import pandas as pd
import yfinance as yf
import pyodbc
import getLastLoadedDate
from datetime import datetime, timedelta

with open(r"C:\Automated-US-Stock-ETL-Pipeline-Project\logs\logs.txt", "a", encoding="utf-8") as f:
    # Define tickers in a list
    tickers = ["AMD", "NVDA", "AMZN", "PLTR", "TSLA", "AAPL"]
    f.write('Tickers\n')

    # set the start date for our market data request
    last_loaded_date = getLastLoadedDate.getLastLoadedDate('us_stocks')
    start_date = datetime(year=2021, month=1, day=1)

    if last_loaded_date != None :
        start_date = last_loaded_date[0] + timedelta(days=1)
   
    # Define interval for fetching data
    interval = "1d"

    f.write('Download start\n')
    data = yf.download(tickers, start=start_date, interval=interval, progress=False)
    f.write('Download done.\n')
    df = data.stack(level=1, future_stack=True).reset_index()

    if df.empty :
        f.write('No new data to load.\n')
        exit()

    df.columns = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = df[['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    most_recent_date = max(df['Date']).date()
    most_recent_date_str = most_recent_date.strftime('%Y-%m-%d')

    f.write('db connect\n')
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=;"
        "DATABASE=USStocks;"
        "UID=;"
        "PWD=;"
    )
    cursor = conn.cursor()
    cursor.fast_executemany = True

    try :
        insert_query = "INSERT INTO raw.us_stocks (Ticker, [Date], [Open], [High], [Low], [Close], Volume) VALUES (?, ?, ?, ?, ?, ?, ?)"

        f.write('Loading Raw Data into SQL Server...\n')
        
        rows = list(df.itertuples(index=False, name=None))
        cursor.executemany(insert_query, rows)
        conn.commit()
        f.write(f"{datetime.now()} - Loaded {len(df)} records\n")
       
        update_query = "EXEC analytics.sp_upsert_etl_last_loaded @tableName=?, @last_loaded_date=?"
        cursor.execute(update_query, ('us_stocks', most_recent_date))
        conn.commit()
        f.write(f'Data from {start_date} - {most_recent_date} has been loaded to raw.us_stocks table.\n') 

    except Exception as e :
        f.write(f"Error : {e}\n")

    finally:
        cursor.close()
        conn.close()
        f.close()