from google.cloud import bigquery

client = bigquery.Client()

# Write query results to a new table
job_config = bigquery.QueryJobConfig()
table_ref = client.dataset("2_trusted").table("graph_liaison")
job_config.destination = table_ref
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

with open(f'pipeline/1_make_json_graph/graph_liaison.sql', "r") as f :
    query = f.read()
f.close()

query_job = client.query(
    query,
    location='eu', # Location must match dataset
    job_config=job_config)
rows = list(query_job)  # Waits for the query to finish


# Export table to GCS
destination_uri = "gs://exercice-python-de-bucket/1_make_json_graph/graph.json"
dataset_ref = client.dataset("2_trusted", project="test01-lincoln-project")
table_ref = dataset_ref.table("graph_liaison")

ejc = bigquery.ExtractJobConfig()
ejc.destination_format='NEWLINE_DELIMITED_JSON'

extract_job = client.extract_table(
    table_ref,
    destination_uri,
    location='eu',
    job_config=ejc)
extract_job.result()  # Waits for job to compl
