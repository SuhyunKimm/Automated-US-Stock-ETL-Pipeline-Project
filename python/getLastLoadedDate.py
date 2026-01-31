# getLastLoadedDate.py
import pyodbc

def getLastLoadedDate (tableName) :
    
    result = -1

    with open(r"C:\Automated-US-Stock-ETL-Pipeline-Project\logs\logs.txt", "a") as f:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=;"
            "DATABASE=USStocks;"
            "UID=;"
            "PWD=;"
        )
        cursor = conn.cursor()

        try:
            sql_query = 'SELECT last_loaded_date FROM analytics.etl_last_loaded WHERE tableName = \'' + tableName + '\';'
            cursor.execute(sql_query)

            result = cursor.fetchone()
        
        except Exception as e :
            f.write(f"Error : {e}\n")
            return -1

        finally:
            if 'cursor' in locals() :
                cursor.close()
            if 'conn' in locals():
                conn.close()
            f.close()
            return result