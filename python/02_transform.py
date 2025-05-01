# All libraries
import pandas as pd
import numpy as np
import os
from typing import Iterator, Union, List, Optional
from pathlib import Path
import shutil
from datetime import date

# Today's Date for Timestamp
current_date = date.today().strftime("%Y-%m-%d")  # Format: YYYY-MM-DD; Get the current date in the desired format

# File paths
parquet_filename = f"../planning_data/database/database_cleaned.parquet"
database_archive = f"../planning_data/database/archive/database_cleaned_{current_date}.parquet" # Append the date to the filename
long_parquet_name = f"../planning_data/database/database_cleaned_long.parquet"
parquet_intermediates =f"../planning_data/ped_report_parquet"
parquet_intermediate_archive =f"../planning_data/ped_report_parquet/archive"
calendar_filename =f"../planning_data/database/calendar.parquet"

# Read in current database, then save and timestamp old database + archive
df_database = pd.read_parquet(parquet_filename, engine="pyarrow")
df_database.to_parquet(database_archive, engine="pyarrow", index=False)
df_database

# Process all new files and combine them all.
dfs = []
parquet_files = [f for f in os.listdir(parquet_intermediates) if f.endswith('.parquet')]

for file in parquet_files:
    file_path = os.path.join(parquet_intermediates, file)
    arch_path = os.path.join(parquet_intermediate_archive, file)
    try:
        df = pd.read_parquet(file_path, engine="pyarrow")
        print(f"Successfully read: {file}")
        dfs.append(df)
        print(f"Successfully appended: {file}")
        dest = shutil.move(file_path, arch_path) 
    except Exception as e:
        print(f"Error reading {file}: {e}")
else:
    print("No valid data to save.")
if dfs:
    # Concatenate all DataFrames into one
    combined_df = pd.concat(dfs, ignore_index=True)
else:
    print("No Parquet files to process.")


# Remove data dumps and replace them with NA
if dfs:
    df = combined_df\
        .drop_duplicates(subset=['From'], keep='first')\
        .reset_index(drop = True)
    df.iloc[:, 2:] = df.iloc[:, 2:].mask(df.iloc[:, 2:] > 500, np.nan)

# Append if new data added
if dfs:
    re_concatenate = [df,df_database]
else:
    re_concatenate = [df_database]

combined_df = pd.concat(re_concatenate, ignore_index=True)
combined_df = combined_df.drop_duplicates()


# Save new database
combined_df.to_parquet(parquet_filename, engine="pyarrow", index=False)

# Combine the first two columns with the filtered data
combined_df["Time_Bucket"] = combined_df["From"].dt.floor("30T")  # "30T" means 30-minute interval
grouped_df = combined_df.groupby("Time_Bucket").sum(numeric_only=True).reset_index()
long_df = grouped_df.melt(id_vars=["Time_Bucket"], var_name="Location", value_name="Count")
long_df.to_parquet(long_parquet_name, engine="pyarrow", index=False)

# Convert 'Time_Bucket' to datetime

# Get the min/max dates
min_date = long_df['date'].min()
max_date = long_df["date"].max()

# Generate date range
date_range = pd.date_range(start=min_date, end=max_date)

# Create DataFrame
df_calendar = pd.DataFrame({
    "date_id": date_range.date,
    "week_of_year": date_range.isocalendar().week,
    "day_of_year": date_range.dayofyear,
    "day_of_week": date_range.strftime('%A'),
    "month_num": date_range.month,
    "day_type": ['Weekend' if d.weekday() in [5, 6] else 'Weekday' for d in date_range],
    "month_name": date_range.strftime('%B'),
    "month_short": date_range.strftime('%b'),
    "quarter": date_range.quarter,
    "year": date_range.year,
    "year_quarter": [f"{y}-Q{q}" for y, q in zip(date_range.year, date_range.quarter)],
    "month_year": date_range.strftime('%b %Y')
}).reset_index(drop = True)
df_calendar.to_parquet(calendar_filename, engine="pyarrow", index=False)
df_calendar