"""
Python Extract Transform load Example
"""
# %%
import requests
import pandas as pd
from sqlalchemy import create_engine

def extract()-> dict:
    """ This API extracts data from
    http://universities.hipolabs.com
    """
    API_URL = "https://financialmodelingprep.com/stable/company-screener?apikey=r1KQHeZcpzyaBtHWcfO8hwuyrMBQ1Dal"
    data = requests.get(API_URL).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    """ 
    Transforms the dataset into desired structure and filters
    """
    df = pd.DataFrame(data)
    print(f"Total Number of stocks from API {len(data)}")
    df = df.dropna(subset=['price'])
    df = df.fillna('')
    print(f"Total Number of stocks from API after drop prices {len(df)}")

    df = df.reset_index(drop=True)
    return df

def load(df:pd.DataFrame)-> None:
    """ Loads data into a sqllite database"""
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('tech_stocks', disk_engine, if_exists='replace')

# %%
data = extract()
df = transform(data)
load(df)

# %%
