import finnhub
import pandas as pd
import numpy as np
import pyodbc

finnhub_client = finnhub.Client(api_key="")

with open(r"C:\Automated-US-Stock-ETL-Pipeline-Project\logs\logs.txt", "a") as f:
    tickers = ["AMD", "NVDA", "AMZN", "PLTR", "TSLA", "AAPL"]
    profile = []

    for t in tickers :
        data = finnhub_client.company_profile2(symbol=t)
        if data :
            profile.append(data)

    df = pd.DataFrame(profile)

    df = df[['ticker', 'name', 'country', 'finnhubIndustry', 'exchange', 'currency']]
    df.rename(columns = 
        {'name' : 'company_name', 
        'finnhubIndustry' : 'industry', 
        'exchange' : 'market'},
        inplace = True)
    df

    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=;"
        "DATABASE=USStocks;"
        "UID=;"
        "PWD=;"
    )
    cursor = conn.cursor()

    try :
        insert_query = "INSERT INTO clean.dim_ticker (ticker, company_name, country, industry, market, currency) VALUES (?, ?, ?, ?, ?, ?)"
        
        f.write("Loading US stock ticker data into SQL Server...")
        
        rows = list(df.itertuples(index=False, name=None))
        cursor.fast_executemany = True
        cursor.executemany(insert_query, rows)
        conn.commit()
        f.write(f"Successfully inserted {len(df)} records.") 

    except Exception as e :
        f.write(f"Error : {e}")

    finally:
        cursor.close()
        conn.close()
        f.close()