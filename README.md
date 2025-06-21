# Career Path Analysis Pipeline

This repository contains the code for the **Career Path Analysis** thesis project at the University of Amsterdam (UvA) Accountancy program. The goal is to build a reproducible, automated data pipeline that can be easily prepared for deployment in UvA’s Azure infrastructure—that:

1. **Discovers** LinkedIn profile URLs for Accountancy alumni using Google’s Custom Search API.
2. **Scrapes** detailed career data from those profiles via the [StaffSpy](https://github.com/cullenwatson/StaffSpy) library.
3. **Orchestrates** the workflow with Apache Airflow, ensuring robust scheduling, monitoring, and extensibility. Airflow is an open source tool defined entirely in Python, allowing for dynamic, extensible and flexible pipelines.

---

## Architecture Overview

The solution is split into two Airflow DAGs:

1. **Batch Search DAG** (`linkedin_batch_dag`):

   * Reads a CSV of alumni names from Azure Data Lake Gen2.
    * The CSV files should conform to a standard format with four columns: Name, Program, Type and Graduation date. The name column should have the name plus surname of the students, to find the correct LinkedIn profiles with more accuracy. 
   * Uses Google’s Custom Search API to find up to N LinkedIn profile URLs per name. (https://developers.google.com/custom-search/v1/overview)
   * Writes the results (with a `linkedin_urls` column) back to a blob container (`output-csv-results`).
   * Triggers the Scraper DAG, passing the new CSV filename via `TriggerDagRunOperator`.
   * For now, Custom Search API is configured to search for 100 names, as it is the limit amount of queries allowed in the free tier. This can be increased with a paid subscription, up to 10000 queries per day. The variable `batch_size` in the `complete_pipeline.py` file changes the amount of queries.

2. **Scraper DAG** (`linkedin_scraper_dag`):

   * Reuses a `session.pkl` file to log in headlessly to LinkedIn.
   * Reads a specific CSV of URLs and extracts the LinkedIn IDs.
   * Calls `StaffSpy.LinkedInAccount.scrape_users(...)` per profile in a `try/except` loop, skipping missing IDs.
   * Concatenates all successful scrapes and writes the final dataset to an Azure container (`scraped-data`).
   * Persists the updated `session.pkl` back to storage for future runs.
   * For testing purposes, the scraper is triggered right after the Batch Search DAG is done. This is due to the aforementioned 100 searched names by Google Custom API. Circumventing LinkedIn's detection system is difficult, and after some tests, we managed to successfully scrape around 150 names per session, twice per day. In case the number of queries by Google Custom API is increased, it is advised to limit the number of pages scraped per session, as an increased number of urls being scraped would likely result in an account timeout.

---

## Prerequisites & Setup

### 1. Google Custom Search API

1. Sign in at the [Google Programmable Search Engine](https://programmablesearchengine.google.com/controlpanel/all).
2. **Enable** the Custom Search API in your project.
3. Create an **API Key** under **APIs & Services → Credentials**.
4. Create a **Custom Search Engine** at [cse.google.com](https://cse.google.com), limit it to search `site:linkedin.com`.
5. Note down your `SEARCH_ENGINE_ID` (CX) from the CSE control panel.

Set these as environment variables (or Airflow Connections):

```bash
env GOOGLE_API_KEY=YOUR_GOOGLE_API_KEY
env SEARCH_ENGINE_ID=YOUR_SEARCH_ENGINE_ID
```

### 2. StaffSpy LinkedIn Scraper

We leverage [StaffSpy by Cullen Watson](https://github.com/cullenwatson/StaffSpy) for LinkedIn scraping.

```bash
pip install staffspy  # or staffspy[browser] for interactive login
```

In Airflow, store LinkedIn credentials securely:

* **Option A**: Airflow Connection `linkedin_creds` (Generic) with `login` and `password`.
* **Option B**: Environment variables `LINKEDIN_USERNAME` and `LINKEDIN_PASSWORD`.

If LinkedIn presents CAPTCHAs during login, sign up for a solver (e.g. 2Captcha or CapSolver), store your key in `CAPSOLVER_API_KEY` or via a Connection, and pass it into `LinkedInAccount` as:

```python
account = LinkedInAccount(
  session_file="session.pkl",
  username=..., password=...,  
  solver_service=SolverType.CAP_SOLVER,
  solver_api_key=os.getenv("CAPSOLVER_API_KEY")
)
```

Note: the solvers are offered as paid services. Some of the possible solutions to guarantee automatic login to Linkedin is:

1. Pay/Implement a captcha solver
2. Locally, run the scraper code and manually login. After you enter a first time, a `session.pkl` file is created. The file is simply a serialized snapshot of your authenticated session state—primarily the cookies and any authentication tokens that StaffSpy has stored after you successfully log in once. After your first session, simply copy the `session.pkl` file into a place where Airflow can easily access and reuse it.

### 3. Azure & Airflow

* **Azure Storage**: Use ADLS Gen2 containers `csv-files`, `output-csv-results`, and `scraped-data`.
* **Airflow Connections**:

  * `AZURE_DATA_LAKE_GEN2` → **wasbs\://** URI with account key.
  * `linkedin_creds` → Generic connection for LinkedIn login.
* **Dependencies**: Add to `requirements.txt`:

  ```text
  apache-airflow-providers-microsoft-azure
  staffspy
  requests
  pandas
  ```
* **Deploy** on Astronomer or Azure Kubernetes Service—Airflow workers will pick up DAGs and env vars automatically.

---

## Running the Pipeline

1. **Deploy** your Airflow project (e.g. `astro deploy`).
2. **Set** initial Airflow Variable `dataset_offset = 0`.
3. **Schedule**: The Batch DAG runs daily, discovers new profiles, and triggers the Scraper DAG.
4. **Monitor**: Use the Airflow UI to track DAG runs, tasks, and XComs.

---

## Extensibility

* Add additional columns (e.g. degree, year) to the search query for more precise results.
* Swap out the LinkedIn scraper for another library by modifying the `scrape_users` task.
* Integrate downstream analytics or visualization tools (Power BI, Azure Synapse).

---

*This project demonstrates a scalable, code-first approach to data engineering using Airflow, Azure, and Python libraries—ideal for UvA’s modern cloud environment.*
