import io
import os
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago
from staffspy import LinkedInAccount, SolverType

# Your existing search function (slightly adapted to accept num_results)
GOOGLE_API_KEY   = os.environ["GOOGLE_API_KEY"]
SEARCH_ENGINE_ID = os.environ["SEARCH_ENGINE_ID"]

def linkedin_profile_search(query: str,
                            university_names: list = ["University of Amsterdam","Universiteit van Amsterdam"],
                            course_names:   list = ["Accountancy"],
                            num_results:    int  = 5):
    search_terms = [f'"{query}"']
    search_terms.append("(" + " OR ".join(f'"{u}"' for u in university_names) + ")")
    search_terms.append("(" + " OR ".join(f'"{c}"' for c in course_names) + ")")
    q = " AND ".join(search_terms) + " site:linkedin.com/in/"

    params = {
        "key": GOOGLE_API_KEY,
        "cx":  SEARCH_ENGINE_ID,
        "q":   q,
        "num": max(1, min(num_results, 10)),  # Google caps at 10
    }

    resp = requests.get("https://www.googleapis.com/customsearch/v1", params=params)
    resp.raise_for_status()
    items = resp.json().get("items") or []
    return [item.get("formattedUrl") for item in items]

# --- DAG 1: Fetch names, search LinkedIn, upload CSV, then trigger scraper DAG ---
@dag(
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["linkedin", "batch"],
    dagrun_timeout=timedelta(hours=1)
)
def linkedin_batch_dag():

    @task
    def fetch_and_search():
        offset = int(Variable.get("dataset_offset", default_var=0))
        batch_size = 5

        hook = WasbHook(wasb_conn_id="AIRFLOW_CONN_AZURE_DATA_LAKE_GEN2")
        raw = hook.read_file(
            container_name="csv-files",
            blob_name="example_processed_names.csv"
        )
        df = pd.read_csv(io.StringIO(raw))

        batch = df.iloc[offset: offset + batch_size].copy()
        batch['linkedin_urls'] = batch['Name'].apply(
            lambda name: ';'.join(linkedin_profile_search(name))
        )

        output_blob = f"linkedin_results_{offset}_{offset + batch_size}.csv"
        hook.load_string(
            container_name="output-csv-results",
            blob_name=output_blob,
            string_data=batch.to_csv(index=False),
            overwrite=True
        )

        Variable.set("dataset_offset", offset + batch_size)
        return output_blob

    search_task = fetch_and_search()

    trigger = TriggerDagRunOperator(
        task_id="trigger_linkedin_scraper",
        trigger_dag_id="linkedin_scraper_dag",
        conf={"target_blob": "{{ ti.xcom_pull(task_ids='fetch_and_search') }}"}
    )

    search_task >> trigger

# instantiate the batch DAG
batch_dag = linkedin_batch_dag()

# --- DAG 2: Scrape LinkedIn profiles for a given CSV ---
@dag(
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["linkedin", "scrape"],
    dagrun_timeout=timedelta(hours=2)
)
def linkedin_scraper_dag():

    @task
    def scrape_specific_file(target_blob: str):
        hook = WasbHook(wasb_conn_id="AIRFLOW_CONN_AZURE_DATA_LAKE_GEN2")

        # Download session.pkl in binary
        downloader = hook.download(
            container_name="output-csv-results",
            blob_name="session.pkl"
        )
        session_path = Path("/tmp/session.pkl")
        session_path.write_bytes(downloader.readall())

        raw_csv = hook.read_file(
            container_name="output-csv-results",
            blob_name=target_blob
        )
        df = pd.read_csv(io.StringIO(raw_csv)).head(5)

        account = LinkedInAccount(
            session_file=str(session_path),
            log_level=1,
        )

        all_dfs = []
        for urls_cell in df.get("linkedin_urls", []):
            if pd.isna(urls_cell) or not urls_cell:
                continue
            urls_list = [u.strip() for u in str(urls_cell).split(";") if u.strip()]
            for url in urls_list:
                try:
                    user_id = url.split("linkedin.com/in/")[1].rstrip("/")
                    res_df = account.scrape_users(user_ids=[user_id])
                    all_dfs.append(res_df)
                except Exception:
                    continue

        final_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

        # Re-upload updated session.pkl
        hook.load_file(
            file_path=str(session_path),
            container_name="output-csv-results",
            blob_name="session.pkl",
            overwrite=True
        )

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_name = f"linkedin_scraped_{timestamp}.csv"
        hook.load_string(
            container_name="scraped-data",
            blob_name=out_name,
            string_data=final_df.to_csv(index=False),
            overwrite=True
        )
        return out_name

    scrape_specific_file("{{ dag_run.conf['target_blob'] }}")

# instantiate the scraper DAG
scraper_dag = linkedin_scraper_dag()
