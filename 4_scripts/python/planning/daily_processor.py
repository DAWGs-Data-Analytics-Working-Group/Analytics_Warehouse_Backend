import pandas as pd
import numpy as np
import datetime as dt
import os
from typing import Union, List, Optional
from pathlib import Path
import shutil
import logging

# Set up logging for this module
logging.getLogger(__name__).addHandler(logging.NullHandler()) # Prevents "No handlers could be found for logger"

# --- Configuration (Relative Subdirectories) ---
# These paths are relative to the 'root_folder' that needs to be passed in.
PLANNING_DATA_SUBDIR = 'Planning'
AWARE_FOLDER_SUBDIR = '3_Analytics_Warehouse_Backend'
DATABASE_LOCATION_SUBDIR = AWARE_FOLDER_SUBDIR + '/5_database'
COLUMN_NAMES_LOCATION_SUBDIR = AWARE_FOLDER_SUBDIR + '/1_inputs'
ANNUAL_REPORT_DATA_SUBDIR = PLANNING_DATA_SUBDIR + '/2_annual_report'

# --- Helper Functions ---

def _load_column_names(file_path: Path) -> List[str]:
    """Loads column names from a CSV file."""
    try:
        cols = pd.read_csv(file_path)["Column Names"].tolist()
        logging.debug(f"Successfully loaded column names from {file_path.name}")
        return cols
    except Exception as e:
        logging.error(f"Error loading column names from {file_path}: {e}")
        return []

def _read_parquet_with_columns(file_path: Path, columns: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
    """Reads a Parquet file, optionally specifying columns."""
    try:
        if columns:
            df = pd.read_parquet(file_path, columns=columns, engine="pyarrow")
        else:
            df = pd.read_parquet(file_path, engine="pyarrow")
        logging.debug(f"Successfully read parquet: {file_path.name}")
        return df
    except Exception as e:
        logging.error(f"Error reading parquet file {file_path}: {e}")
        return None

def _read_csv_with_melt(file_path: Path, id_vars: str, var_name: str, value_name: str, drop_cols: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
    """Reads a CSV, optionally drops columns, and melts it."""
    try:
        df = pd.read_csv(file_path)
        if drop_cols:
            df = df.drop(drop_cols, axis=1, errors="ignore")
        df = df.melt(id_vars=id_vars, var_name=var_name, value_name=value_name)
        logging.debug(f"Successfully read and melted CSV: {file_path.name}")
        return df
    except Exception as e:
        logging.error(f"Error reading or melting CSV file {file_path}: {e}")
        return None

# --- Main Daily Processing Function ---

def process_daily_data(root_folder: Union[str, Path]):
    """
    Processes daily entry data by combining P1 sensor data with historical data,
    and generates a calendar table.

    Args:
        root_folder (Union[str, Path]): The absolute path to the root directory
                                        containing 'planning' and '3_Analytics_Warehouse_Backend'.
    """
    logging.info("Starting daily data processing and calendar generation...")

    # root_folder = Path(root_folder).resolve()#.parents[4]

    # Define absolute paths
    planning_data = root_folder / PLANNING_DATA_SUBDIR
    aware_folder = root_folder / AWARE_FOLDER_SUBDIR
    database_location = root_folder / DATABASE_LOCATION_SUBDIR
    column_names_location = root_folder / COLUMN_NAMES_LOCATION_SUBDIR
    annual_report_data_path = root_folder / ANNUAL_REPORT_DATA_SUBDIR

    # Define specific file paths
    database_path = database_location / 'database_cleaned_p1.parquet'
    daily_path = annual_report_data_path / 'pre_covid_daily_entries.csv'
    daily_path_database = database_location / 'daily.parquet'
    calendar_filename = database_location / 'calendar.parquet'
    sensor_mapping_path = planning_data / 'Sensor_Mapping.csv'

    # --- Load Data ---
    logging.info("Loading necessary data files...")
    cols_p1 = _load_column_names(column_names_location / 'columns.csv')
    sensors = pd.read_csv(sensor_mapping_path) # Sensor mapping usually doesn't change often

    if not cols_p1:
        logging.critical("P1 column names not loaded. Exiting daily data processing.")
        return
    if sensors.empty:
        logging.critical("Sensor mapping data not loaded or is empty. Exiting daily data processing.")
        return

    # Process database_cleaned_p1.parquet
    df_database_p1 = _read_parquet_with_columns(database_path, columns=cols_p1)
    if df_database_p1 is None or df_database_p1.empty:
        logging.error(f"P1 database not found or is empty at {database_path}. Skipping daily processing.")
        return

    try:
        df_database = (
            df_database_p1.drop("To Time", axis=1, errors="ignore")
            .assign(From=lambda df: pd.to_datetime(df["From"], errors='coerce').dt.date) # Ensure datetime first, then date
            .melt(id_vars=["From"], var_name="Location", value_name="Count")
            .merge(sensors, left_on="Location", right_on="Sensor Names", how='left') # Use left merge to keep all P1 entries
        )
        logging.info("Processed P1 database for daily entries.")
    except Exception as e:
        logging.error(f"Error processing P1 database for daily entries: {e}")
        return

    # Filter and prepare dailys from P1 database
    cols_for_dailys = ["From", "Location", "Count", "Hub In/Out"]
    dailys = df_database[cols_for_dailys]
    dailys = dailys[dailys["Hub In/Out"] == "IN"]
    dailys = dailys.drop("Hub In/Out", axis=1) # Drop after filtering
    dailys["Count"] = pd.to_numeric(dailys["Count"], errors="coerce") # Ensure numeric
    dailys = dailys.groupby(["From", "Location"], as_index=False).agg(Count=("Count", "sum"))
    logging.info(f"Prepared {len(dailys)} daily records from P1 database.")

    # Process pre_covid_daily_entries.csv
    df_pre_covid = _read_csv_with_melt(
        daily_path, id_vars="From", var_name="Location", value_name="Count", drop_cols=["To Time"]
    )
    if df_pre_covid is None or df_pre_covid.empty:
        logging.warning(f"Pre-COVID daily entries CSV not found or is empty at {daily_path}. Skipping its inclusion.")
        df = pd.DataFrame(columns=["From", "Location", "Count", "Hub In/Out"]) # Create empty df to prevent errors
    else:
        try:
            df = df_pre_covid.merge(sensors, left_on="Location", right_on="Sensor Names", how='left')
            cols_for_df = ["From", "Location", "Count", "Hub In/Out"] # Re-select columns after merge
            df = df[cols_for_df]
            df = df[df["Hub In/Out"] == "IN"]
            df = df.drop("Hub In/Out", axis=1).dropna().reset_index(drop=True)
            df["Count"] = pd.to_numeric(df["Count"], errors="coerce") # Ensure numeric
            logging.info(f"Prepared {len(df)} daily records from pre-COVID data.")
        except Exception as e:
            logging.error(f"Error processing pre-COVID daily entries: {e}")
            df = pd.DataFrame(columns=["From", "Location", "Count"]) # Empty dataframe on error


    # --- Combine Daily Data ---
    logging.info("Combining historical and current daily data...")
    if not dailys.empty and not df.empty:
        daily_database = pd.concat([df, dailys], ignore_index=True)
    elif not dailys.empty:
        daily_database = dailys
    elif not df.empty:
        daily_database = df
    else:
        logging.warning("No daily data to combine. Skipping daily database creation.")
        return

    daily_database['From'] = pd.to_datetime(daily_database['From'], errors='coerce').dt.date
    daily_database = daily_database.dropna(subset=['From', 'Count']) # Drop rows where 'From' or 'Count' became NaN after conversion
    daily_database = daily_database.groupby(['From', 'Location'], as_index=False).agg(Count=('Count', 'sum')) # Aggregate again after concat
    
    # Save combined daily database
    try:
        daily_database.to_parquet(daily_path_database, engine="pyarrow", index=False)
        logging.info(f"Saved combined daily database to {daily_path_database.name} with {len(daily_database)} records.")
    except Exception as e:
        logging.error(f"Error saving combined daily database: {e}")

    # --- Generate Calendar Table ---
    logging.info("Generating calendar table...")
    if daily_database.empty:
        logging.warning("Daily database is empty, cannot generate calendar table.")
        return

    min_date = daily_database['From'].min()
    max_date = daily_database["From"].max()

    if pd.isna(min_date) or pd.isna(max_date):
        logging.warning("Min or max date for calendar generation is NaN. Skipping calendar table creation.")
        return

    try:
        date_range = pd.date_range(start=min_date, end=max_date)

        iso = date_range.isocalendar()

        df_calendar = pd.DataFrame({
            "date_id": date_range.date,

            # ISO calendar (NEW)
            "iso_year": iso.year.astype(int),
            "iso_week": iso.week.astype(int),
            "iso_day": iso.day.astype(int),  # optional but useful (Mon=1)

            # Existing fields
            "week_of_year": iso.week.astype(int),  # if you want to keep this
            "day_of_year": date_range.dayofyear,
            "day_of_week": date_range.strftime('%A'),
            "day_of_week_num": date_range.weekday,  # Mon=0 .. Sun=6
            "month_num": date_range.month,
            "day_type": ['Weekend' if d.weekday() in [5, 6] else 'Weekday' for d in date_range],
            "month_name": date_range.strftime('%B'),
            "month_short": date_range.strftime('%b'),
            "quarter": date_range.quarter,
            "year": date_range.year,
            "year_quarter": [f"{y}-Q{q}" for y, q in zip(date_range.year, date_range.quarter)],
            "month_year": date_range.strftime('%b %Y')
        }).reset_index(drop=True)

        df_calendar.to_parquet(calendar_filename, engine="pyarrow", index=False)
        logging.info(f"Saved calendar table to {calendar_filename.name} with {len(df_calendar)} entries.")
    except Exception as e:
        logging.error(f"Error generating or saving calendar table: {e}")

    logging.info("\n--- Daily data processing and calendar generation complete. ---")


# Optional: Add a simple execution block if this file is run directly for testing
if __name__ == "__main__":
    # This block will only execute if daily_processor.py is run directly

    # Configure logging for direct execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Running daily_processor.py directly for testing.")

    # Determine root_folder based on the current script's location
    # If daily_processor.py is in 'your_project_root/3_Analytics_Warehouse_Backend/4_scripts/python/planning/'
    # The root folder is 4 levels up.
    # From the script: 4_scripts/python/planning/daily_processor.py
    # 1. 4_scripts/python/planning/
    # 2. 4_scripts/python/
    # 3. 4_scripts/
    # 4. 3_Analytics_Warehouse_Backend/
    # 5. your_project_root/
    # So, it's parents[4]
    
    # Corrected root_folder determination based on the provided traceback path
    # c:\Users\schew\OneDrive - The Port Authority of New York & New Jersey\Planning\01_Data Repository Analytics and Reporting\1_WTC_A-Ware\3_Analytics_Warehouse_Backend\4_scripts\python\planning\data_appender.py
    # The traceback shows data_appender.py is in '.../4_scripts/python/planning/'.
    # If daily_processor.py is in the same directory, then the root_folder is indeed parents[4] from the script's location.

    root_folder_for_testing = Path(__file__).resolve().parents[4]

    logging.info(f"Root folder for testing: {root_folder_for_testing}")

    # Call the main function
    process_daily_data(root_folder_for_testing)