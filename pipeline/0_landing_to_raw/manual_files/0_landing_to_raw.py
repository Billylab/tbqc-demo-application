"""
This script manage data from GCS Landing to BQ Raw.
""" 

# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------

from datetime import datetime,timezone
import json
import pandas as pd
import yaml
import os, sys
from google.cloud import storage


currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
grandparentdir = os.path.dirname(parentdir)
sys.path.append(grandparentdir)

from utils.gcs_functions import gcs_delete_list_blobs
from utils.quality_functions import quality_validation_to_gcs, quality_stats_gcs_to_bq
from utils.gcs_to_bq_functions import gcs_to_bq_load
from utils.utils_functions import read_file_csv_length

ENV = "dev"

# --------------------------------------------------------------------------------
# Fetch YAML pipeline arguments
# --------------------------------------------------------------------------------

with open(f'pipeline/0_landing_to_raw/manual_files/config.yml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

# Tables
TABLES = config.get('tables')

# Data Quality
DQ_GCS_PROJECT_ID = config.get('data_quality').get('gcs_project_id') + ENV
DQ_GCS_DATA_PATH = config.get('data_quality').get('gcs_data_path')
DQ_GCS_FILE_STATS_FILE_NAME = config.get('data_quality').get('gcs_file_stats_filename')
DQ_GCS_ROWS_STATS_FILE_NAME = config.get('data_quality').get('gcs_rows_stats_filename')
DQ_GCS_DATA_INVALID_FILE_NAME = config.get('data_quality').get('gcs_data_invalid_filename')

DQ_BQ_PROJECT_ID = config.get('data_quality').get('bq_project_id') + ENV
DQ_BQ_DATASET_NAME = config.get('data_quality').get('bq_dataset_name')
DQ_BQ_FILE_STATS_TABLE_NAME = config.get('data_quality').get('bq_file_stats_table_name')
DQ_BQ_ROWS_STATS_TABLE_NAME = config.get('data_quality').get('bq_rows_stats_table_name')
DQ_BQ_DATA_INVALID_TABLE_NAME = config.get('data_quality').get('bq_data_invalid_table_name')


# --------------------------------------------------------------------------------
# Define functions
# --------------------------------------------------------------------------------

def fun_gcs_data_valid_to_bq(source_project_id, source_bucket_name, source_path, filename, destination_project_id, destination_dataset_name, destination_table_name, schema, write_mode):
    print(f'----- From project {source_project_id} and bucket {source_bucket_name}, load {source_path}{filename}.csv to BQ {destination_project_id}.{destination_dataset_name}.{destination_table_name} : write_mode {write_mode}-----')
    
    source_gcs_client = storage.Client(project=source_project_id)
    source_bucket = source_gcs_client.get_bucket(source_bucket_name)
    blob = source_bucket.get_blob(f'{source_path}{filename}.csv')
    blob_data_length = read_file_csv_length(blob, separator=',', encoding='utf-8')

    if blob_data_length > 0 :
        gcs_to_bq_load(
            source_project_id=source_project_id, 
            source_bucket_name=source_bucket_name, 
            source_path=source_path, 
            filename=filename, 
            destination_project_id=destination_project_id,
            destination_dataset_name=destination_dataset_name, 
            destination_table_name=destination_table_name, 
            file_info={'format':'csv'}, 
            schema=schema, 
            write_mode=write_mode
        )
    else :
        print(f"File {source_path}{filename}.csv has no line.")

    # gcs_delete_list_blobs(project_id=source_project_id, bucket_name=source_bucket_name, source_path=source_path, file_prefix=f'{filename}.csv')

###### Start of Local Test #######

for table in TABLES :    

    to_request = TABLES.get(table).get('to_request')
    source_gcs_project_id = TABLES.get(table).get('source_gcs_project_id') + ENV
    source_gcs_bucket_name = TABLES.get(table).get('source_gcs_bucket_name')
    source_file_params = TABLES.get(table).get('source_file_params')
    destination_bq_project_id = TABLES.get(table).get('destination_bq_project_id') + ENV
    destination_bq_dataset_name = TABLES.get(table).get('destination_bq_dataset_name')
    destination_bq_table_name = TABLES.get(table).get('destination_bq_table_name')
    write_mode = TABLES.get(table).get('write_mode')

    if to_request == False :
        print(f'-- Use case {table} has been ignored')
    
    else : 
        with open(source_file_params) as params_file: 
            params = json.load(params_file)
        params_file.close()
        schema_df = pd.DataFrame(params.get('schema'))
        col_to_ignore = ['source_filename', 'execution_datetime']
        col_name_to_check = schema_df[~schema_df.name.isin(col_to_ignore)].expected_file_name.to_list()
        bq_schema_df = schema_df[['name', 'type', 'mode']]
        bq_schema_json = bq_schema_df.to_dict(orient='records')

        print('============ DATA QUALITY ============')
        current_date=datetime.now()
        utc_ts = current_date.astimezone(timezone.utc)
        print("utc ingestion date",utc_ts.strftime("%Y-%m-%dT%H:%M:%SZ"))


        quality_validation_to_gcs(
            pipeline_name="0_landing_to_raw", 
            dataset_name=destination_bq_dataset_name,
            table_name=destination_bq_table_name,        
            core_landing_project_id=source_gcs_project_id, 
            core_landing_bucket_name=source_gcs_bucket_name, 
            core_landing_source_path=params["source_path"], 
            core_exploitation_project_id=source_gcs_project_id, 
            core_exploitation_bucket_name=source_gcs_bucket_name, 
            core_exploitation_dq_stats_path=f'{DQ_GCS_DATA_PATH}/{table}/', 
            filename_file_stats=DQ_GCS_FILE_STATS_FILE_NAME,
            filename_rows_stats=DQ_GCS_ROWS_STATS_FILE_NAME,
            filename_data_invalid=DQ_GCS_DATA_INVALID_FILE_NAME,
            filename_data_valid=destination_bq_table_name,
            params=params, 
            ts=utc_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        print('============ GCS DATA QUALITY FILES TO BQ ============')
        quality_stats_gcs_to_bq(
            source_project_id=source_gcs_project_id, 
            source_bucket_name=source_gcs_bucket_name, 
            source_path=f'{DQ_GCS_DATA_PATH}/{table}/', 
            filename_file_stats=DQ_GCS_FILE_STATS_FILE_NAME,
            filename_rows_stats=DQ_GCS_ROWS_STATS_FILE_NAME,
            filename_data_invalid=DQ_GCS_DATA_INVALID_FILE_NAME,
            destination_project_id=destination_bq_project_id,
            destination_dataset_name=DQ_BQ_DATASET_NAME, 
            destination_table_name_file_stats=DQ_BQ_FILE_STATS_TABLE_NAME, 
            destination_table_name_rows_stats=DQ_BQ_ROWS_STATS_TABLE_NAME, 
            destination_table_name_data_invalid=DQ_BQ_DATA_INVALID_TABLE_NAME
        )

        print('============ GCS DATA VALID TO BQ ============')
        fun_gcs_data_valid_to_bq(
            source_project_id=source_gcs_bucket_name, 
            source_bucket_name=source_gcs_bucket_name, 
            source_path=f'{DQ_GCS_DATA_PATH}/{table}/', 
            filename=destination_bq_table_name, 
            destination_project_id=destination_bq_project_id,
            destination_dataset_name=destination_bq_dataset_name, 
            destination_table_name=destination_bq_table_name, 
            schema=params.get('schema'), 
            write_mode=write_mode
        )