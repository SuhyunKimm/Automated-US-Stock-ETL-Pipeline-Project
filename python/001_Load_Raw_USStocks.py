import pandas as pd
import yfinance as yf
import pyodbc
from datetime import datetime

with open(r"C:\Automated-US-Stock-ETL-Pipeline-Project\logs\logs.txt", "a") as f:
    # Define tickers in a list
    tickers = ["AMD", "NVDA", "AMZN", "PLTR", "TSLA", "AAPL"]
    f.write('Tickers\n')

    # set the start date for our market data request
    start_date = datetime(year=2021, month=1, day=1)

    # Define interval for fetching data
    interval = "1d"

    f.write('Download start\n')
    data = yf.download(tickers, start=start_date, interval=interval, progress=False)
    f.write('Download done.\n')
    df = data.stack(level=1, future_stack=True).reset_index()

    df.columns = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = df[['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

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
        f.write('insert\n')
        insert_query = "INSERT INTO raw.us_stocks (Ticker, [Date], [Open], [High], [Low], [Close], Volume) VALUES (?, ?, ?, ?, ?, ?, ?)"
        
        f.write('Loading Raw Data into SQL Server...\n')
        
        rows = list(df.itertuples(index=False, name=None))
        cursor.executemany(insert_query, rows)
        conn.commit()
        f.write(f"{datetime.now()} - Loaded {len(df)} records\n")


    except Exception as e :
        f.write(f"Error : {e}\n")

    finally:
        cursor.close()
        conn.close()
        f.close()