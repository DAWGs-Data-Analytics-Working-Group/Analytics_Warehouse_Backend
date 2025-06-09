import pandas as pd
import numpy as np
import os
# import gc
from typing import Iterator, Union, List, Optional
from pathlib import Path
import shutil
from sqlalchemy import create_engine
from sqlalchemy import URL

df_database = pd.read_parquet("../planning_data/database/database_cleaned_long.parquet", engine="pyarrow")
# Split into separate columns
df_database['date'] = df_database['Time_Bucket'].dt.date#.astype('datetime64[ns]')
df_database['time'] = df_database['Time_Bucket'].dt.time
df_database = df_database.drop(columns=['Time_Bucket'])

df_database.columns = [c.lower() for c in df_database.columns] #postgres doesn't like capitals or spaces
df_database.columns = df_database.columns.str.replace('[^a-zA-Z0-9]', '').str.lower()
df_database.columns = df_database.columns.str.replace("/", '_')
df_database.columns = df_database.columns.str.replace(r' ', '_')
df_database.columns = df_database.columns.str.replace('-', '')
df_database.columns = df_database.columns.str.strip('_')

url_object = URL.create(
    "postgresql",
    username="postgres",
    password="DeathLife!@#",  # plain (unescaped) tex
    host="localhost",
    database="Xovis_Counts",
)
engine = create_engine(url_object)

df_database.to_sql("database_cleaned", engine, if_exists='replace', index=False)
