# your_function_app_name/ProcessFullCsv/__init__.py

import logging
import json
import os
import pandas as pd
from io import BytesIO, StringIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient

# Assuming Google Search_util.py is in the same folder as this function's __init__.py
from . import google_search_util

# Environment variables for Google API
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
SEARCH_ENGINE_ID = os.environ.get("SEARCH_ENGINE_ID")

# Azure Storage Connection String (AzureWebJobsStorage is typically set automatically by Function App)
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AzureWebJobsStorage")

blob_service_client = None
if AZURE_STORAGE_CONNECTION_STRING:
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    except Exception as e:
        logging.error(f"Failed to initialize BlobServiceClient: {e}", exc_info=True)
else:
    logging.error("AZURE_STORAGE_CONNECTION_STRING environment variable is not set. Blob operations will fail.")

OUTPUT_CONTAINER_NAME = "output-csv-results" # Consistent with previous discussions

def main(myblob: func.InputStream, name: str):
    logging.info(f"Python blob trigger function (ProcessFullCsv) processed blob\n"
                 f"Name: {name}\n"
                 f"Blob Size: {myblob.length} Bytes")

    if blob_service_client is None:
        logging.error("BlobServiceClient not initialized. Exiting function.")
        return
    if not GOOGLE_API_KEY or not SEARCH_ENGINE_ID:
        logging.error("Google API Key or Search Engine ID is missing in Function App settings. Exiting function.")
        return

    try:
        # Extract container name from the blob path
        blob_path_parts = myblob.name.split('/')
        input_container_name = blob_path_parts[0]
        input_blob_name = '/'.join(blob_path_parts[1:]) # Reconstruct blob name if it has folders

        if not input_blob_name.lower().endswith(".csv"):
            logging.info(f"Skipping non-CSV blob: {name}")
            return # Only process CSVs

        # Ensure output container exists
        output_container_client = blob_service_client.get_container_client(OUTPUT_CONTAINER_NAME)
        try:
            output_container_client.create_container()
            logging.info(f"Output container '{OUTPUT_CONTAINER_NAME}' created or already exists.")
        except Exception as e:
            logging.warning(f"Could not ensure output container '{OUTPUT_CONTAINER_NAME}' exists: {e}")

        # Read the entire CSV into an in-memory stream for pandas
        csv_buffer = BytesIO(myblob.read())
        
        # Read the entire CSV into a DataFrame. pandas automatically handles the header.
        full_df = pd.read_csv(csv_buffer)
        
        if full_df.empty:
            logging.info(f"Input CSV '{input_blob_name}' is empty. No processing needed.")
            return

        logging.info(f"Processing '{input_blob_name}'. Total data rows: {len(full_df)}.")

        # Initialize new column for LinkedIn URLs
        full_df['linkedin_url'] = None

        # Iterate through the DataFrame and perform search for each row
        # Using iterrows() for simplicity, though it can be slow for very large DFs
        # For huge DFs, vectorization or apply() might be considered, but iterrows is clear.
        for index, row in full_df.iterrows():
            person_name = row['Name'] # Assuming 'Name' column exists
            program_name = row.get('Program') # Use .get to safely access, assuming 'Program' might not always exist

            university_names = [program_name] if pd.notna(program_name) else []
            course_names = [] # You can populate this if your CSV has course info

            try:
                linkedin_profile_url = google_search_util.linkedin_profile_search(
                    query=person_name,
                    university_names=university_names,
                    course_names=course_names,
                    api_key=GOOGLE_API_KEY,
                    search_engine_id=SEARCH_ENGINE_ID
                )
                full_df.loc[index, 'linkedin_url'] = linkedin_profile_url
                logging.info(f"Searched '{person_name}'. Found: {linkedin_profile_url or 'None'}")
            except Exception as search_e:
                logging.error(f"Search error for '{person_name}' in '{input_blob_name}': {search_e}", exc_info=True)
                full_df.loc[index, 'linkedin_url'] = f"ERROR: {str(search_e)}" # Store error in the column

        # --- Save Processed Data to Output CSV ---
        output_blob_name = f"processed_{os.path.splitext(input_blob_name)[0]}.csv"
        output_blob_client = blob_service_client.get_blob_client(
            container=OUTPUT_CONTAINER_NAME, blob=output_blob_name
        )

        output_csv_buffer = StringIO()
        full_df.to_csv(output_csv_buffer, index=False) # Write the entire DataFrame to CSV
        
        # Upload the processed CSV data, overwriting if it exists (for full re-processing)
        output_blob_client.upload_blob(output_csv_buffer.getvalue(), overwrite=True)
        logging.info(f"Successfully saved processed data to '{output_blob_name}'.")

    except Exception as e:
        logging.error(f"Error processing full CSV '{name}': {e}", exc_info=True)
        # In a simpler flow, no state update on error, just log it.
        # If you need to track errors, consider a simple log file or dead-letter queue.

    logging.info("ProcessFullCsv function finished.")
