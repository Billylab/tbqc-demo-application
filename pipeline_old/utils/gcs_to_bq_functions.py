from google.cloud import bigquery, storage
import io
from utils.utils_functions import excel_file_to_csv_string


def gcs_to_bq_load(source_project_id:str, source_bucket_name:str, source_path:str, destination_dataset_name: str, destination_table_name:str, filename:str, file_info:dict, schema:list, write_mode:str, destination_project_id:str=None):
    """
    Load file from a bucket to destination dataset table.
    Entries :
        - source_project_id           (string, required): The destination project identifier
        - source_bucket_name          (string, required): The destination bucket name
        - source_path                 (string, required): The source object path 
        - destination_dataset_name    (string, required): The BigQuery dataset to load data into
        - destination_table_name      (string, required): The BigQuery table to load data into
        - filename                    (string, required): The filename to load
        - file_info                   (dict, required): The file information
        - schema                      (list, required): The schema field list
        - write_mode                  (string, required): The write mode to BigQuery
        - destination_project_id      (string, optional): The source project identifier. If NULL, the same as destination project id
    """
    gcs_client_source = storage.Client(project=source_project_id)
    bucket_source = gcs_client_source.bucket(bucket_name=source_bucket_name)

    bq_client = bigquery.Client(project=destination_project_id) if destination_project_id else bigquery.Client(project=source_project_id)
    table_id = f'{destination_project_id}.{destination_dataset_name}.{destination_table_name}' if destination_project_id else f'{source_project_id}.{destination_dataset_name}.{destination_table_name}'

    if write_mode=='APPEND':
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    if write_mode=='TRUNCATE':
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE


    format = file_info.get('format')
    if format == 'csv' or format == "xlsx":
        
        job_config = bigquery.LoadJobConfig(
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            field_delimiter=';',
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=write_disposition
        )
        if format == 'csv':
            blob = bucket_source.blob(blob_name=f'{source_path}{filename}.csv')
            csv_content = blob.download_as_string()
        elif format == "xlsx":
            csv_content = excel_file_to_csv_string(
                project_id=source_project_id, 
                bucket_name=source_bucket_name, 
                source_path=source_path, 
                file_info=file_info, 
                filename=filename
            ).encode('utf-8')

        load_job = bq_client.load_table_from_file(
            file_obj=io.BytesIO(csv_content), 
            destination=table_id, 
            job_config=job_config
        ) # Make an API request
        load_job.result() # Waits for the job to complete
        print(f'Successfully loaded rows.')