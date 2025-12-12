import pandas as pd
import numpy as np
import os
from typing import Union, List, Optional # <--- Ensure Optional is explicitly imported
from pathlib import Path
import shutil
from datetime import date
import logging
# Set up logging for this module
logging.getLogger(__name__).addHandler(logging.NullHandler()) # Prevents "No handlers could be found for logger"

# --- Configuration (Relative Subdirectories) ---
# These paths are relative to the 'root_folder' that needs to be passed into the main function.
PLANNING_DATA_SUBDIR = 'planning'
AWARE_FOLDER_SUBDIR = '3_Analytics_Warehouse_Backend'

COLUMN_NAMES_LOCATION_SUBDIR = AWARE_FOLDER_SUBDIR + '/1_inputs'
PARQUET_INTERMEDIATE_LOCATION_SUBDIR = AWARE_FOLDER_SUBDIR + '/2_planning_processing_files'
DATABASE_LOCATION_SUBDIR = AWARE_FOLDER_SUBDIR + '/5_database'

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

def _archive_dataframe(df: pd.DataFrame, base_path: Path, file_name: str, current_date_str: str):
    """Archives a DataFrame with a timestamp."""
    archive_dir = base_path / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_file_path = archive_dir / f"{file_name}_{current_date_str}.parquet"
    try:
        df.to_parquet(archive_file_path, engine="pyarrow", index=False)
        logging.info(f"Archived database to: {archive_file_path.name}")
    except Exception as e:
        logging.error(f"Error archiving {file_name} to {archive_file_path}: {e}")

def _process_new_parquet_files(source_dir: Path, processed_dir: Path, columns: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
    """
    Processes new parquet files from a source directory, moves them to a processed directory,
    and concatenates them into a single DataFrame.
    """
    dfs = []
    parquet_files = [f for f in os.listdir(source_dir) if f.endswith('.parquet')]
    processed_dir.mkdir(parents=True, exist_ok=True) # Ensure processed dir exists

    if not parquet_files:
        logging.info(f"No new parquet files found in {source_dir.name}.")
        return None

    for file_name in parquet_files:
        file_path = source_dir / file_name
        arch_path = processed_dir / file_name
        try:
            df = _read_parquet_with_columns(file_path, columns)
            if df is not None:
                dfs.append(df)
                logging.info(f"Appended new data from: {file_name}")
                shutil.move(str(file_path), str(arch_path))
                logging.info(f"Moved '{file_name}' to processed folder.")
        except Exception as e:
            logging.error(f"Error processing new parquet file {file_name}: {e}")

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return None

def _clean_and_append_p1_p2(new_df: Optional[pd.DataFrame], existing_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans new P1/P2 data, removes duplicates, and appends to the existing database."""
    combined_df = existing_df

    if new_df is not None and not new_df.empty:
        # Remove data dumps / outliers > 500
        temp_df = new_df.drop_duplicates(subset=['From'], keep='first').reset_index(drop=True)
        # Apply mask only if there are columns to mask (i.e., beyond 'From', 'To', etc.)
        if temp_df.shape[1] > 2: # Assuming first two columns are 'From', 'To' or similar identifiers
             numeric_cols = temp_df.select_dtypes(include=np.number).columns
             temp_df[numeric_cols] = temp_df[numeric_cols].mask(temp_df[numeric_cols] > 4000, np.nan)
        
        combined_df = pd.concat([temp_df, existing_df], ignore_index=True)
        logging.info(f"Appended {len(temp_df)} new records to database.")
    else:
        logging.info("No new P1/P2 data to append.")

    # Always clean and sort the combined database
    combined_df = combined_df.groupby(["From", "To Time"], as_index=False).mean().sort_values("From")
    # combined_df = combined_df.drop_duplicates().sort_values(by="From").reset_index(drop=True)

    return combined_df

def _clean_and_append_veh(new_df: Optional[pd.DataFrame], existing_df: pd.DataFrame) -> pd.DataFrame:
    """Cleans new Vehicle data, removes duplicates, and appends to the existing database."""
    combined_df = existing_df

    if new_df is not None and not new_df.empty:
        # Ensure consistent column names before concat for veh_df if necessary
        # (This was handled in the previous script 'process_traffic_data')
        
        combined_df = pd.concat([new_df, existing_df], ignore_index=True)
        logging.info(f"Appended {len(new_df)} new records to vehicle database.")
    else:
        logging.info("No new Vehicle data to append.")

    # Always clean and sort the combined database
    combined_df = combined_df.drop_duplicates().sort_values(by=["arrive_date", "arrive_time"]).reset_index(drop=True)
    
    return combined_df

# --- Main Append/Archive/Load Function ---

def append_archive_load_data(root_folder: Union[str, Path]):
    """
    Reads in new data, appends it to current databases, transforms into a long format
    for BI, and archives old data for version control.

    Args:
        root_folder (Union[str, Path]): The absolute path to the root directory
                                        containing 'planning' and '3_Analytics_Warehouse_Backend'.
    """
    logging.info("Starting data append, archive, and load process...")
    root_folder = Path(__file__).resolve().parents[4]    
    current_date_str = date.today().strftime("%Y-%m-%d")

    # Define absolute paths
    column_names_location = root_folder / COLUMN_NAMES_LOCATION_SUBDIR
    parquet_intermediate_location = root_folder / PARQUET_INTERMEDIATE_LOCATION_SUBDIR
    database_location = root_folder / DATABASE_LOCATION_SUBDIR

    # Ensure archive directory exists
    (database_location / "archive").mkdir(parents=True, exist_ok=True)


    # --- Load Column Names ---
    cols_p1 = _load_column_names(column_names_location / "columns.csv")
    cols_p2 = _load_column_names(column_names_location / "p2_columns.csv")
    cols_veh = _load_column_names(column_names_location / "veh_cols.csv")

    if not all([cols_p1, cols_p2, cols_veh]):
        logging.critical("Failed to load all required column names for appending. Exiting.")
        return

    # --- Read in Current Databases and Archive ---
    logging.info("Reading current databases and archiving old versions...")
    df_database_p1 = _read_parquet_with_columns(database_location / 'database_cleaned_p1.parquet', columns=cols_p1)
    df_database_p2 = _read_parquet_with_columns(database_location / 'database_cleaned_p2.parquet', columns=cols_p2)
    df_database_veh = _read_parquet_with_columns(database_location / 'database_veh.parquet') # veh_cols are for ingestion, not the final df structure

    if df_database_p1 is None or df_database_p2 is None or df_database_veh is None:
        logging.critical("Failed to load one or more existing databases. Cannot proceed with appending.")
        return

    _archive_dataframe(df_database_p1, database_location, "database_cleaned_p1", current_date_str)
    _archive_dataframe(df_database_p2, database_location, "database_cleaned_p2", current_date_str)
    _archive_dataframe(df_database_veh, database_location, "database_veh", current_date_str)

    # --- Process New P1 Sensor Data ---
    logging.info("\n--- Processing new P1 sensor data ---")
    new_p1_df = _process_new_parquet_files(
        parquet_intermediate_location / "p1_sensors_parquet",
        parquet_intermediate_location / "p1_sensors_parquet" / "processed",
        columns=cols_p1 # Pass columns to ensure consistent schema on new files if needed
    )
    combined_p1_df = _clean_and_append_p1_p2(new_p1_df, df_database_p1)
    combined_p1_df.to_parquet(database_location / "database_cleaned_p1.parquet", engine="pyarrow", index=False)
    logging.info(f"Saved updated P1 database with {len(combined_p1_df)} records.")

    # --- Process New P2 Sensor Data ---
    logging.info("\n--- Processing new P2 sensor data ---")
    new_p2_df = _process_new_parquet_files(
        parquet_intermediate_location / "p2_sensors_parquet",
        parquet_intermediate_location / "p2_sensors_parquet" / "processed",
        columns=cols_p2 # Pass columns
    )
    combined_p2_df = _clean_and_append_p1_p2(new_p2_df, df_database_p2)

    # Additional P2 specific cleaning (masking values > 500)
    if combined_p2_df.shape[1] > 2:
        numeric_cols_p2 = combined_p2_df.select_dtypes(include=np.number).columns
        combined_p2_df[numeric_cols_p2] = combined_p2_df[numeric_cols_p2].mask(combined_p2_df[numeric_cols_p2] > 4000, np.nan)

    combined_p2_df.to_parquet(database_location / "database_cleaned_p2.parquet", engine="pyarrow", index=False)
    logging.info(f"Saved updated P2 database with {len(combined_p2_df)} records.")


    # --- Combine P1 and P2 Data for Long Format BI ---
    logging.info("\n--- Combining P1 and P2 data for long format BI ---")
    # Ensure 'From' and 'To Time' are datetime objects
    if 'From' in combined_p1_df.columns:
        combined_p1_df['From'] = pd.to_datetime(combined_p1_df['From'], errors='coerce')
    if 'From' in combined_p2_df.columns: # Assuming 'To Time' in your original code referred to 'From' in P2 for merge
        combined_p2_df['From'] = pd.to_datetime(combined_p2_df['From'], errors='coerce')


    long_df_p1 = None
    if 'From' in combined_p1_df.columns and combined_p1_df['From'].notna().any():
        p1_numeric_cols = combined_p1_df.select_dtypes(include=np.number).columns
        if not p1_numeric_cols.empty:
            temp_p1_df = combined_p1_df[['From'] + p1_numeric_cols.tolist()].copy()
            temp_p1_df["Time_Bucket"] = temp_p1_df["From"].dt.floor("30min")
            grouped_p1_df = temp_p1_df.groupby("Time_Bucket")[p1_numeric_cols].sum().reset_index()
            long_df_p1 = grouped_p1_df.melt(id_vars=["Time_Bucket"], var_name="Location", value_name="Count")
            # long_df_p1['Sensor_Type'] = 'P1'
        else:
            logging.warning("No numeric columns found in P1 data for melting.")
    else:
        logging.warning("P1 combined DataFrame does not have a 'From' column or valid time data for BI processing.")

    long_df_p2 = None
    if 'From' in combined_p2_df.columns and combined_p2_df['From'].notna().any():
        p2_numeric_cols = combined_p2_df.select_dtypes(include=np.number).columns
        if not p2_numeric_cols.empty:
            temp_p2_df = combined_p2_df[['From'] + p2_numeric_cols.tolist()].copy()
            temp_p2_df["Time_Bucket"] = temp_p2_df["From"].dt.floor("30min")
            grouped_p2_df = temp_p2_df.groupby("Time_Bucket")[p2_numeric_cols].sum().reset_index()
            long_df_p2 = grouped_p2_df.melt(id_vars=["Time_Bucket"], var_name="Location", value_name="Count")
            # long_df_p2['Sensor_Type'] = 'P2'
        else:
            logging.warning("No numeric columns found in P2 data for melting.")
    else:
        logging.warning("P2 combined DataFrame does not have a 'From' column or valid time data for BI processing.")

    final_long_dfs = []
    if long_df_p1 is not None and not long_df_p1.empty:
        final_long_dfs.append(long_df_p1)
    if long_df_p2 is not None and not long_df_p2.empty:
        final_long_dfs.append(long_df_p2)

    if final_long_dfs:
        combined_long_df = pd.concat(final_long_dfs, ignore_index=True)
        combined_long_df['date'] = combined_long_df['Time_Bucket'].dt.date
        combined_long_df['time'] = combined_long_df['Time_Bucket'].dt.time
        combined_long_df = combined_long_df.drop(columns=['Time_Bucket'])
        combined_long_df = combined_long_df.drop_duplicates().sort_values(by="date").reset_index(drop=True)
        combined_long_df.to_parquet(database_location / "database_cleaned_long.parquet", engine="pyarrow", index=False)
        logging.info(f"Saved combined long format database with {len(combined_long_df)} records.")
    else:
        logging.warning("No valid P1 or P2 data to create combined long format database.")


    # --- Process New Vehicle Data ---
    logging.info("\n--- Processing new Vehicle data ---")
    new_veh_df = _process_new_parquet_files(
        parquet_intermediate_location / "veh_parquet",
        parquet_intermediate_location / "veh_parquet" / "processed"
    )
    combined_veh_df = _clean_and_append_veh(new_veh_df, df_database_veh)
    combined_veh_df.to_parquet(database_location / "database_veh.parquet", engine="pyarrow", index=False)
    logging.info(f"Saved updated Vehicle database with {len(combined_veh_df)} records.")

    logging.info("\n--- Data append, archive, and load process complete. ---")


# Optional: Add a simple execution block if this file is run directly for testing
if __name__ == "__main__":
    # This block will only execute if data_appender.py is run directly

    # Configure logging for direct execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Running data_appender.py directly for testing.")

    # Determine root_folder based on the current script's location
    # Assuming this script is at 'your_project_root/3_Analytics_Warehouse_Backend/2_planning_processing_files/data_appender.py'
    script_location = Path(__file__).resolve()
    # intermediate_dir = script_location.parent # Path to '2_planning_processing_files'
    # aware_dir = intermediate_dir.parent      # Path to '3_Analytics_Warehouse_Backend'
    root_folder = Path(__file__).resolve().parents[4]
    logging.info(f"Root folder for testing: {root_folder}")

    # Call the main function
    append_archive_load_data(root_folder)