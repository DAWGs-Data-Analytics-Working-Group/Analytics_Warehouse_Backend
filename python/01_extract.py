import pandas as pd
import os
from typing import Iterator, Union, List, Optional
from pathlib import Path
import shutil

# Read the CSV and extract column names as a list
cols = pd.read_csv("../columns.csv")["Column Names"].tolist()

xlsx_files = [f for f in os.listdir('../planning_data/Monthly Ped Reports') if f.endswith(('.xlsx', '.xlsm'))]

for file in xlsx_files:
    file_path = os.path.join('../planning_data/Monthly Ped Reports', file)
    arch_path = os.path.join('../planning_data/Monthly Ped Reports/processed', file)

    try:
        # Read the Excel file with specified columns
        df = pd.read_excel(file_path, sheet_name="Data", usecols=cols, engine="openpyxl")
        df = df[df["From"].notnull()]
        print(f"Successfully read: {file}")
        # Generate the parquet filename dynamically
        parquet_filename = f"../planning_data/ped_report_parquet/{file.rsplit('.', 1)[0]}.parquet"
        # Save as Parquet
        df.to_parquet(parquet_filename, engine="pyarrow", index=False)
        print(f"Parquet file saved: {parquet_filename}")
        # Move the original file to archive
        dest = shutil.move(file_path, arch_path) 

    except Exception as e:
        print(f"Error reading {file}: {e}")
else:
    print("No valid data to save.")
