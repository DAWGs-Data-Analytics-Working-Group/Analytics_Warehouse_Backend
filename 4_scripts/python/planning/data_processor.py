import pandas as pd
import numpy as np
import os
from typing import Iterator, Union, List, Optional
from pathlib import Path
import shutil
import pytz
import logging

# --- Configuration ---
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

root_folder = Path(__file__).resolve().parents[4] # More robust way to get root folder
planning_data = root_folder / 'planning'
aware_folder = root_folder / '3_Analytics_Warehouse_Backend'

traffic_email_path = planning_data / '3_emails_from_traffic'
phase2_sensor_path = planning_data / '4_phase_2_sensors' / 'P2'

column_names_location = aware_folder / '1_inputs'
parquet_intermediate_location = aware_folder / '2_planning_processing_files'

clear_folder = traffic_email_path / 'clear'
processed_traffic_email_folder = traffic_email_path / 'processed'
processed_phase2_sensor_folder = phase2_sensor_path / 'processed'

# Ensure output directories exist
for folder in [clear_folder, processed_traffic_email_folder, processed_phase2_sensor_folder,
               parquet_intermediate_location / 'p1_sensors_parquet',
               parquet_intermediate_location / 'p2_sensors_parquet',
               parquet_intermediate_location / 'veh_parquet']:
    folder.mkdir(parents=True, exist_ok=True)


# --- Helper Functions ---
def load_column_names(file_name: Path) -> List[str]:
    """Loads column names from a CSV file."""
    try:
        return pd.read_csv(file_name)["Column Names"].tolist()
    except Exception as e:
        logging.error(f"Error loading column names from {file_name}: {e}")
        return []

def move_file(src: Path, dst: Path):
    """Moves a file from source to destination."""
    try:
        shutil.move(str(src), str(dst))
        logging.info(f"Moved '{src.name}' to '{dst}'")
    except Exception as e:
        logging.error(f"Error moving file {src.name} to {dst}: {e}")

# --- Main Processing Logic ---

def process_traffic_data():
    """Orchestrates the processing of various traffic data files."""

    logging.info("Starting traffic data processing...")

    # Load column names
    cols = load_column_names(column_names_location / "columns.csv")
    cols_p2 = load_column_names(column_names_location / "p2_columns.csv")
    veh_cols = load_column_names(column_names_location / "veh_cols.csv")

    if not all([cols, cols_p2, veh_cols]):
        logging.critical("Failed to load all required column names. Exiting.")
        return

    logging.info(f"Loaded P1 Columns (first 3): {cols[:3]}")
    logging.info(f"Loaded P2 Columns (first 3): {cols_p2[:3]}")
    logging.info(f"Loaded Vehicle Columns (first 3): {veh_cols[:3]}")

    # Discover and categorize files
    all_traffic_files = [f for f in os.listdir(traffic_email_path) if f.lower().endswith(('.xlsx', '.xlsm', '.pdf'))]
    all_p2_sensor_files = [f for f in os.listdir(phase2_sensor_path) if f.lower().endswith(('.xlsx', '.xlsm'))]

    excel_files = [f for f in all_traffic_files if f.lower().endswith(('.xlsx', '.xlsm'))]
    pdf_files = [f for f in all_traffic_files if f.lower().endswith('.pdf')]
#'MONTHLY' in f.upper() or
    pedestrian_counts = [f for f in excel_files if  'P1' in f.upper() or 'phase' in f.upper()]
    veh_report_files = [f for f in excel_files if 'WTC_VEH_REPORT' in f.upper()]

    other_excel_files = [f for f in excel_files if f not in pedestrian_counts and f not in veh_report_files]

    files_to_clear = pdf_files + other_excel_files

    logging.info(f"Found P1 files: {[f for f in pedestrian_counts]}")
    logging.info(f"Found P2 files in sensor path: {[f for f in all_p2_sensor_files]}")
    logging.info(f"Found Vehicle report files: {[f for f in veh_report_files]}")
    logging.info(f"Files to move to 'clear' folder: {[f for f in files_to_clear]}")

    # Move specified files to 'clear' folder
    for f in files_to_clear:
        src = traffic_email_path / f
        dst = clear_folder / f
        move_file(src, dst)

    # --- Process Pedestrian (P1) Sensor Files ---
    logging.info("\n--- Processing P1 Sensor Files ---")
    if not pedestrian_counts:
        logging.info("No P1 sensor files found to process.")

    for file in pedestrian_counts:
        file_path = traffic_email_path / file
        arch_path = processed_traffic_email_folder / file
        parquet_filename = parquet_intermediate_location / 'p1_sensors_parquet' / f"{file.rsplit('.', 1)[0]}.parquet"

        try:
            df = pd.read_excel(file_path, sheet_name="Data", usecols=cols, engine="openpyxl")
            df = df[df["From"].notnull()]
            df = df[~df["From"].astype(str).str.contains("screenline list", case=False, na=False)]

            if not df.empty:
                df.to_parquet(parquet_filename, engine="pyarrow", index=False)
                logging.info(f"Successfully processed and saved P1 parquet: {parquet_filename.name}")
                move_file(file_path, arch_path)
            else:
                logging.warning(f"No valid 'From' data found in {file}. Skipping parquet save.")

        except Exception as e:
            logging.error(f"Error processing P1 file {file}: {e}")

    # --- Process Phase 2 (P2) Sensor Files ---
    logging.info("\n--- Processing P2 Sensor Files ---")
    possible_p2_sheets = ["5-Min Data", "Sheet1", "Data"]
    if not all_p2_sensor_files:
        logging.info("No P2 sensor files found to process.")

    for file in all_p2_sensor_files:
        file_path = phase2_sensor_path / file
        arch_path = processed_phase2_sensor_folder / file
        parquet_filename = parquet_intermediate_location / 'p2_sensors_parquet' / f"{file.rsplit('.', 1)[0]}.parquet"

        df = None
        for sheet in possible_p2_sheets:
            try:
                temp_df = pd.read_excel(file_path, sheet_name=sheet, usecols=cols_p2, engine="openpyxl")
                if "From" in temp_df.columns and temp_df["From"].notnull().any():
                    df = temp_df
                    logging.info(f"Successfully read '{file}' using sheet '{sheet}'")
                    break
            except Exception as e:
                logging.debug(f"Could not read sheet '{sheet}' from '{file}': {e}") # Use debug for sheet-specific errors

        if df is None or not ("From" in df.columns and df["From"].notnull().sum() > 0):
            logging.warning(f"No valid data found in {file} across sheets: {possible_p2_sheets}. Skipping.")
            continue

        try:
            df = df[df["From"].notnull()]
            df.to_parquet(parquet_filename, engine="pyarrow", index=False)
            logging.info(f"Successfully processed and saved P2 parquet: {parquet_filename.name}")
            move_file(file_path, arch_path)
        except Exception as e:
            logging.error(f"Error saving or moving P2 file {file}: {e}")

    # --- Process Vehicle Report Files ---
    logging.info("\n--- Processing Vehicle Report Files ---")
    possible_veh_sheets = ["Raw POV Data"]
    if not veh_report_files:
        logging.info("No vehicle report files found to process.")

    for file in veh_report_files:
        file_path = traffic_email_path / file
        arch_path = processed_traffic_email_folder / file
        parquet_path = parquet_intermediate_location / "veh_parquet" / f"{file.rsplit('.', 1)[0]}.parquet"

        df = None
        for sheet in possible_veh_sheets:
            try:
                temp_df = pd.read_excel(file_path, sheet_name=sheet, usecols=veh_cols, engine="openpyxl")
                if "Ticket" in temp_df.columns and temp_df["Ticket"].notnull().any():
                    df = temp_df
                    logging.info(f"Successfully read '{file}' using sheet '{sheet}'")
                    break
            except Exception as e:
                logging.debug(f"Could not read sheet '{sheet}' from '{file}': {e}")

        if df is None or not ("Ticket" in df.columns and df["Ticket"].notnull().sum() > 0):
            logging.warning(f"No valid 'Ticket' data found in {file} across sheets: {possible_veh_sheets}. Skipping.")
            continue

        try:
            df = df[df["Ticket"].notnull()]

            # Standardize column names
            df.columns = (
                df.columns.astype(str).str.strip()
                .str.lower()
                .str.replace(" ", "_")
                .str.replace("/", "_")
                .str.replace(r"[^\w_]", "", regex=True)
            )

            # Ensure 'Vehicle Plate' is all strings (if it exists)
            if "vehicle_plate" in df.columns:
                df["vehicle_plate"] = df["vehicle_plate"].astype(str)

            datetime_fields = {
                "arrived_at": "arrive",
                "grant_deny_at": "grant",
                "vsc_entry": "vsc_entry",
                "gantry_exit": "gantry_exit"
            }

            for source_col, base_name in datetime_fields.items():
                if source_col in df.columns:
                    dt_series = pd.to_datetime(df[source_col], errors='coerce', utc=True)
                    # Convert to US/Eastern and remove timezone info for consistency if needed later
                    # dt_series = dt_series.dt.tz_convert('US/Eastern').dt.tz_localize(None)

                    df[f"{base_name}"] = dt_series
                    df[f"{base_name}_date"] = dt_series.dt.date
                    df[f"{base_name}_time"] = dt_series.dt.time
                    df[f"{base_name}_rounded"] = dt_series.dt.floor("30min").dt.time
                else:
                    logging.warning(f"Datetime column '{source_col}' not found in {file}. Skipping its processing.")

            # Base columns to keep
            base_cols = [
                "ticket", "vehicle_plate", "vehicle_plate_state", "vehicle_type",
                "vendor_tenant_association", "driver_name", "driver_license_state",
                "control_point_name", "ticket_notes", "notes"
            ]

            # Dynamically generated columns
            generated_cols = []
            for _, base in datetime_fields.items():
                generated_cols += [
                    f"{base}_date", f"{base}_time", f"{base}_rounded"
                ]

            # Final selection — only keep relevant columns that exist in the DataFrame
            keep_cols = [col for col in (base_cols + generated_cols) if col in df.columns]
            df = df[keep_cols]

            df.to_parquet(parquet_path, engine="pyarrow", index=False)
            logging.info(f"Successfully processed and saved Vehicle parquet: {parquet_path.name}")
            move_file(file_path, arch_path)

        except Exception as e:
            logging.error(f"Error processing Vehicle report file {file}: {e}")

    logging.info("\n--- Traffic data processing complete. ---")


if __name__ == "__main__":
    process_traffic_data()