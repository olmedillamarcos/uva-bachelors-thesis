import io
import os
from pathlib import Path
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from staffspy import LinkedInAccount

# DAG to read a specific output CSV, extract LinkedIn IDs from URLs, scrape profiles, and store combined results
@dag(
    schedule_interval=None,           # run manually or via TriggerDagRun
    start_date=datetime(2025, 1, 1),  # arbitrary past date
    catchup=False,
    tags=["linkedin", "scrape"],
)
def linkedin_scraper_dag_test():
    @task
    def scrape_specific_file():
        hook = WasbHook(wasb_conn_id="AIRFLOW_CONN_AZURE_DATA_LAKE_GEN2")

        # 1️⃣ Download session.pkl to reuse login as binary
        downloader = hook.download(
            container_name="output-csv-results",
            blob_name="session.pkl"
        )
        session_bytes = downloader.readall()
        session_path = Path("/tmp/session.pkl")
        session_path.write_bytes(session_bytes)

        # 2️⃣ Only process the specific CSV file and limit to 5 rows for testing
        target_blob = "linkedin_results_2857_2862.csv"
        raw_csv = hook.read_file(
            container_name="output-csv-results",
            blob_name=target_blob
        )
        df = pd.read_csv(io.StringIO(raw_csv))
        df = df.head(5)

        # 3️⃣ Init LinkedInAccount (headless, reusing session.pkl)
        account = LinkedInAccount(
            session_file=str(session_path),
            log_level=1,
        )

        all_dfs = []
        # 4️⃣ Extract ID(s) and scrape, skipping failures
        for urls_cell in df.get("linkedin_urls", []):
            if pd.isna(urls_cell) or not urls_cell:
                continue
            # Split semicolon-separated URLs
            urls_list = [u.strip() for u in str(urls_cell).split(";") if u.strip()]
            for url in urls_list:
                try:
                    # extract ID after '/in/'
                    user_id = url.split("linkedin.com/in/")[1].rstrip("/")
                    # scrape returns a DataFrame
                    res_df = account.scrape_users(user_ids=[user_id])
                    all_dfs.append(res_df)
                except Exception:
                    continue

        # 5️⃣ Concatenate all successful results
        final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

        # 6️⃣ Upload combined CSV to 'scraped-data' container
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_name = f"linkedin_scraped_{timestamp}.csv"
        csv_data = final_df.to_csv(index=False)
        hook.load_string(
            container_name="scraped-data",
            blob_name=out_name,
            string_data=csv_data,
            overwrite=True
        )

        return out_name

    scrape_specific_file()


dag = linkedin_scraper_dag_test()
