import os
import logging
from pathlib import Path
import data_processor
import data_appender
import daily_processor
from datetime import datetime # Import datetime for timestamping log files

# --- Main Script Configuration ---

# Define your project root (as determined previously)
project_root = Path(__file__).resolve().parents[4]

# Define the log directory within your '3_Analytics_Warehouse_Backend' folder
log_dir = project_root / '3_Analytics_Warehouse_Backend' / 'logs'
log_dir.mkdir(parents=True, exist_ok=True) # Create the logs directory if it doesn't exist

# Create a timestamp for the log file name
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = log_dir / f"pipeline_run_{timestamp}.log"

# Set up logging for the main script. This will control all logging output.
# IMPORTANT: Only call basicConfig once, typically in your main script.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename, # <--- THIS IS THE KEY CHANGE
    filemode='a' # 'a' for append (default), 'w' for overwrite
)
logging.info("Main script logging configured.")
logging.info(f"Logs are being saved to: {log_filename}")

if __name__ == "__main__":
    # Add a StreamHandler to also print to console while logging to file
    # This is useful so you see output in real-time
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO) # Set console output level
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(console_handler)


    try:
        logging.info("--- Step 1: Processing raw incoming data files ---")
        data_processor.process_traffic_data()
        logging.info("Raw data file processing completed.")

        logging.info("\n--- Step 2: Appending processed data to databases and archiving ---")
        data_appender.append_archive_load_data(project_root)
        logging.info("Data appending, archiving, and loading completed.")

        logging.info("\n--- Step 3: Processing daily entries and generating calendar ---")
        daily_processor.process_daily_data(project_root)
        logging.info("Daily entry processing and calendar generation completed.")

        logging.info("\n--- All data pipeline steps completed successfully! ---")

    except Exception as e:
        logging.critical(f"An unhandled error occurred during the data pipeline: {e}", exc_info=True)