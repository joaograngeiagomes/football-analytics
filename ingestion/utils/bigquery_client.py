import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def get_client() -> bigquery.Client:
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds
    return bigquery.Client(project="football-analytics-492122")

def load_json_to_bigquery(data: list, dataset: str, table: str, schema: list) -> None:
    client = get_client()
    table_ref = f"football-analytics-492122.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    job = client.load_table_from_json(data, table_ref, job_config=job_config)
    job.result()

    print(f"Loaded {len(data)} rows into {table_ref}")