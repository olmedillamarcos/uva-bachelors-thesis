import io
import os
import pandas as pd
import requests

from datetime import timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago

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

  
@dag(
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,        # serialize runs so offset updates don’t race
    tags=["linkedin", "batch"],
)
def linkedin_batch_dag_test():

    @task
    def fetch_and_search():
        # Read & increment offset
        offset = int(Variable.get("dataset_offset", default_var=0))
        batch_size = 5

        # Pull CSV from ADLS Gen2 (via WASB hook)
        hook = WasbHook(wasb_conn_id="AIRFLOW_CONN_AZURE_DATA_LAKE_GEN2")
        raw  = hook.read_file(container_name="csv-files", blob_name="example_processed_names.csv")
        df   = pd.read_csv(io.StringIO(raw))

        # 3Slice out the next 100 rows
        batch = df.iloc[offset : offset + batch_size]

        # Search LinkedIn for each name
        batch['linkedin_urls'] = batch['Name'].apply(
            lambda name: ';'.join(linkedin_profile_search(name))
        )

        csv_data = batch.to_csv(index=False)

        # 5persist results
        output_blob = f"linkedin_results_{offset}_{offset + batch_size}.csv"
        hook.load_string(
            container_name="output-csv-results",
            blob_name=output_blob,
            string_data=csv_data,
            overwrite=True
        )

        # Update the offset for the next run
        next_offset = offset + batch_size
        Variable.set("dataset_offset", next_offset)

        return output_blob

    fetch_and_search()


dag = linkedin_batch_dag_test()
