import requests
from datetime import datetime
import psycopg2
import psycopg2.extras
import time

"""
This script, and the associated functions, act as the driver of a data pipeline that extracts, transforms, and loads data from the TwelveData API
into a PostgreSQL database. First, hourly stock exchange data for the specified stocks and etfs is requested from the TwelveData API. Next, this 
data is modified and transform to meet required specifications. This modified and transformed data is then loaded to a PostgreSQL database. This
process is repeated for various technical indicators of each stock and etf.
"""

def extract(url):
    """
    This function makes a get request to the TwelveData API, and then returns a decoded response.

    Args:
        url (String): The url for the TwelveData API

    Returns:
        Dictionary: Decoded response from making a get request to the TwelveData API
    """
    time_start = time.perf_counter()
    r = requests.get(url)
    print(f"Status code: {r.status_code}")
    response_dict = r.json()
    elapsed_time = time.perf_counter() - time_start
    print(f"Extract time: {elapsed_time:.4f}")
    return response_dict

def transform_hourly(response_dict):
    """
    This function accepts a dictionary object as a parameter, and then transforms the data of interest to ensure proper formatting and typing.
    Specifically, the value of the key "values" in the dictionary parameter is modified, and this value, which is itself a list, is passed to 
    the next function called in the data pipeline. The datetime is changed to a python datetime, the volume type is changed to int, 
    and then the rest of the data is changed to the float type. 

    Args:
        response_dict (Dictionary): Decoded response from get request
    """
    time_start = time.perf_counter()
    for value in response_dict["values"]:
        value["symbol"] = response_dict["meta"]["symbol"]
        value["datetime"] = datetime.strptime(value["datetime"], "%Y-%m-%d %H:%M:%S")
        value["open"] = float(value["open"])
        value["high"] = float(value["high"])
        value["low"] = float(value["low"])
        value["close"] = float(value["close"])
        value["volume"] = int(value["volume"])
    elapsed_time = time.perf_counter() - time_start
    print(f"Transform time: {elapsed_time:.4f}")
    load_hourly(response_dict["values"])
    
def transform_technical(response_dict, indicator):
    """
    This function accepts a dictionary object and a string as a parameters, and then modifies and transforms the input data
    to ensure proper formatting and typing. Specifically, it creates a list of dictionaries, and then fills the dictionaries
    with modified and transformed data from the dictionary parameter. The list of dictionaries is then passed the next function
    in the data pipeline.

    Args:
        response_dict (Dictionary): Decoded response from get request
        indicator (String): The technical indicator that the data refers to
    """
    time_start = time.perf_counter()
    value_list = []
    for value in response_dict["values"]:
        value_dict = {}
        value_dict["symbol"] = response_dict["meta"]["symbol"]
        value_dict["datetime"] = datetime.strptime(value["datetime"], "%Y-%m-%d %H:%M:%S")
        value_dict["indicator"] = list(value.keys())[1]
        value_dict["value"] = float(value[indicator])
        value_list.append(value_dict)
    elapsed_time = time.perf_counter() - time_start
    print(f"Transform time: {elapsed_time:.4f}")
    load_technical(value_list)

def create_hourly_table(curr):
    """
    This function uses the cursor object parameter to query the database for the existence of a table named "hourly_data". If this table is present,
    nothing else happens. If this table is not present, then the cursor object will execute SQL code to create the appropiate table in the database.

    Args:
        curr (Object): Psycopg2 cursor object using a connection to the database
    """
    curr.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = %s)", ("hourly_data",))
    exists = curr.fetchone()[0]
    if not exists:
        curr.execute("""
                    CREATE TABLE hourly_data (
                        ticker VARCHAR(10) NOT NULL,
                        datetime TIMESTAMP NOT NULL,
                        open DECIMAL(10,2),
                        high DECIMAL(10,2),
                        low DECIMAL(10,2),
                        close DECIMAL(10,2),
                        volume BIGINT,
                        PRIMARY KEY (ticker, datetime)                   
                    );
                    """)
        
def create_technical_table(curr):
    """
    This function uses the cursor object parameter to query the database for the existence of a table named "technical_data". If this table is present,
    nothing else happens. If this table is not present, then the cursor object will execute SQL code to create the appropiate table in the database.

    Args:
        curr (Object): Psycopg2 cursor object using a connection to the database
    """
    curr.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = %s)", ("technical_data",))
    exists = curr.fetchone()[0]
    if not exists:
        curr.execute("""
                    CREATE TABLE technical_data (
                        ticker VARCHAR(10) NOT NULL,
                        datetime TIMESTAMP NOT NULL,
                        indicator VARCHAR(10) NOT NULL,
                        value DECIMAL(10,2),
                        PRIMARY KEY (ticker, datetime, indicator)                   
                    );
                    """)
    
def load_hourly(hourly_data):
    """
    This function creates a connection to the database, calls "create_hourly_table" to create this table should it not already be present, and then executes
    a batch insert into the "hourly_data" table. The SQL insert statement uses key value pairs from the list of dictionaries parameter in order to pass named
    arguments into the SQL command.

    Args:
        hourly_data (List): The modified and transformed value of the key "values" from the decoded response dictionary
    """
    time_start = time.perf_counter()
    conn = psycopg2.connect(dbname="YOUR_DB_NAME", user="postgres", password="YOUR_PASSWORD", host="localhost", port="PORT_NUMBER")
    curr = conn.cursor()
    create_hourly_table(curr)
    psycopg2.extras.execute_batch(curr,
                                  """
                                  INSERT INTO hourly_data VALUES (
                                      %(symbol)s,
                                      %(datetime)s,
                                      %(open)s,
                                      %(high)s,
                                      %(low)s,
                                      %(close)s,
                                      %(volume)s      
                                  );
                                  """, hourly_data)
    conn.commit()
    curr.close()
    conn.close()
    elapsed_time = time.perf_counter() - time_start
    print(f"Load time: {elapsed_time:.4f}")
    
def load_technical(technical_data):
    """
    This function creates a connection to the database, calls "create_technical_table" to create this table should it not already be present, and then executes
    a batch insert into the "technical_data" table. The SQL insert statement uses key value pairs from the list of dictionaries parameter in order to pass named
    arguments into the SQL command.

    Args:
        technical_data (List): The relevant, transformed data from the decoded response dictionary
    """
    time_start = time.perf_counter()
    conn = psycopg2.connect(dbname="YOUR_DB_NAME", user="postgres", password="YOUR_PASSWORD", host="localhost", port="PORT_NUMBER")
    curr = conn.cursor()
    create_technical_table(curr)
    psycopg2.extras.execute_batch(curr,
                                  """
                                  INSERT INTO technical_data VALUES (
                                      %(symbol)s,
                                      %(datetime)s,
                                      %(indicator)s,
                                      %(value)s     
                                  );
                                  """, technical_data)
    conn.commit()
    curr.close()
    conn.close()
    elapsed_time = time.perf_counter() - time_start
    print(f"Load time: {elapsed_time:.4f}")
    
# Script start
        
tickers = ["SPY", "XOM", "USDX", "VIXY", "GLD", "QQQ", "ARKK", "IBIT"]

date = datetime.today().strftime("%Y-%m-%d")

for ticker in tickers:
    url = "https://api.twelvedata.com/time_series?apikey=YOUR_API_KEY&interval=30min&symbol=" + ticker + "&outputsize=13&date=today"
    data = extract(url)
    transform_hourly(data)
    
time.sleep(100) # To accomodate API limits

technical_indicators = ["adx", "rsi", "percent_b", "ema"]

for ticker in tickers: 
    for technical_indicator in technical_indicators:
        url = "https://api.twelvedata.com/" + technical_indicator + "?apikey=YOUR_API_KEY&interval=30min&symbol=" + ticker + "&outputsize=13&start_date=" + date
        data = extract(url)
        transform_technical(data, technical_indicator)
        time.sleep(60) # To accomodate API limits