from google.cloud import storage
import pandas as pd
import json
import sys


def main():
    gcs_project_id = "test01-lincoln-project"
    gcs_bucket_name = "exercice-python-de-bucket"

    # Define GCS objects
    gcs_client = storage.Client(project=gcs_project_id)
    gcs_bucket = gcs_client.bucket(bucket_name=gcs_bucket_name)
    blob = gcs_bucket.get_blob(blob_name=f'1_make_json_graph/graph.json')

    # Transform json lines file
    json_content = blob.download_as_bytes()
    result = [json.loads(jline) for jline in json_content.splitlines()]

    # Unest, deduplicate and count distinct drug by journal
    df = pd.json_normalize(result, meta=["atcode","drug"], record_path=['journal'])
    df = df[['journal','atcode','drug']].drop_duplicates()
    df = df.groupby(['journal'])['journal'].count().reset_index(name='counts')

    # Fetch journals with max count
    column = df["counts"]
    max_value = column.max()
    df = df.where(df.counts == max_value)

    # Return result
    print(list(df['journal']))
    sys.exit(0)
    # return list(df['journal'])

main()