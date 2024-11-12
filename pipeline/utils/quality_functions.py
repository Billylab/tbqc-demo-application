import logging
import pandas as pd
import io
import os
import re
import datetime
import json
import ast
from google.cloud import storage
from utils.gcs_functions import gcs_delete_list_blobs
from utils.gcs_to_bq_functions import gcs_to_bq_load



SCHEMA_TABLE_DATA_INVALID = [
    {'name': 'pipeline', 'type': 'STRING'},
    {'name': 'dataset', 'type': 'STRING'},
    {'name': 'table', 'type': 'STRING'},
    {'name': 'source_filename', 'type': 'STRING'},
    {'name': 'index', 'type': 'INTEGER'},
    {'name': 'invalid_reason', 'type': 'STRING'},
    {'name': 'invalid_code', 'type': 'STRING'},
    {'name': 'invalid_col_name', 'type': 'STRING'},
    {'name': 'invalid_col_value', 'type': 'STRING'},
    {'name': 'date_ingest', 'type': 'DATETIME'}
]

SCHEMA_TABLE_ROWS_STATS = [
    {'name': 'pipeline', 'type': 'STRING'},
    {'name': 'dataset', 'type': 'STRING'},
    {'name': 'table', 'type': 'STRING'},
    {'name': 'source_filename', 'type': 'STRING'},
    {'name': 'date_ingest', 'type': 'DATETIME'},
    {'name': 'nb_valid_lines', 'type': 'INTEGER'},
    {'name': 'nb_invalid_lines', 'type': 'INTEGER'},
    {'name': 'nb_invalid_lines_NULL_VALUE_NOT_EXPECTED', 'type': 'INTEGER'},
    {'name': 'nb_invalid_lines_KEY_NOT_UNIQUE', 'type': 'INTEGER'},
    {'name': 'nb_invalid_lines_COLUMN_NOT_PARSABLE', 'type': 'INTEGER'},  
]

SCHEMA_TABLE_FILE_STATS = [
    {'name': 'pipeline', 'type': 'STRING'},
    {'name': 'dataset', 'type': 'STRING'},
    {'name': 'table', 'type': 'STRING'},
    {'name': 'source_filename', 'type': 'STRING'},
    {'name': 'date_ingest', 'type': 'DATETIME'},
    {'name': 'is_invalid', 'type': 'BOOLEAN'},
    {'name': 'description', 'type': 'STRING'}
]


def check_files(pipeline_name:str, dataset_name: str, table_name:str, blob:str, params:dict, ingestion_date:str):
    """
    Check a blob reprensing a csv or an Excel file : check if it is readable, check if it contains the required columns defined in params.schema
    Entries :
    - pipeline_name                    (str, required) : Name of the pipeline
    - dataset_name                     (str, required) : Name of the dataset
    - table_name                       (str, required) : Name of the table
    - blob                             (blob, required): Blob object
    - params                           (dict, required): Parameters containing at least : file_info {source, name, format, separator, encoding} and schema
    - ingestion_date                   (str, required) : Ingestion date of the file
    Return :
    - df                               df : dataframe containing the data
    - df_file_stats                    df : dataframe containing the stats of the ingested file : pipeline, source_filename, date_ingest, is_invalid, description
    - error_file                       bool : True if there is an error while reading the file
    """
    df = pd.DataFrame()
    df_file_stats = pd.DataFrame()
    error_file = False
    schema_df = pd.DataFrame(params.get('schema'))
    col_to_ignore = ['source_filename', 'execution_datetime']
    col_names_to_check = schema_df[~schema_df.name.isin(col_to_ignore)].expected_file_name.to_list()

    file_format = os.path.splitext(blob.name)[1].lstrip('.')

    if file_format == 'csv':
        print(f"file format is {file_format}")
        separator = params.get('file_info').get('format').get(file_format).get('separator')
        encoding = params.get('file_info').get('format').get(file_format).get('encoding')
    if file_format == 'xlsx' or file_format == 'xlsm':
        print(f"file format is {file_format}")
        encoding = params.get('file_info').get('format').get(file_format).get('encoding')
        sheet_name = params.get('file_info').get('format').get(file_format).get('sheet_name')
        data_columns = params.get('file_info').get('format').get(file_format).get('data_columns')
        data_from_rows = params.get('file_info').get('format').get(file_format).get('data_from_rows')
    if file_format == 'json':
        print(f"file format is {file_format}")

    # Read file
    if file_format == 'csv':
        csv_content = blob.download_as_string()
        try:
            df = pd.read_csv(filepath_or_buffer=io.BytesIO(csv_content), dtype=str, encoding=encoding, sep=separator)
        except Exception as e:
            print(f"Error when reading the CSV file :'{blob.name}' : {e}")
            error_file = True
            description = f'Error when reading the CSV file : {e}'
    if file_format == 'xlsx' or file_format == 'xlsm':
        excel_content = blob.download_as_bytes()
        try:
            df = pd.read_excel(io=excel_content, sheet_name=sheet_name, engine='openpyxl', usecols=data_columns, skiprows=data_from_rows, dtype=str)
        except: 
            print(f"Error when reading the Excel file :'{blob.name}' : {e}")
            error_file = True
            description = f'Error when reading the Excel file : {e}'
    if file_format == 'json':
        json_content = blob.download_as_string()
        try : 
            df = pd.read_json(path_or_buf=io.BytesIO(json_content))
        except: 
            print(f"Error when reading the JSON file :'{blob.name}' : {e}")
            error_file = True
            description = f'Error when reading the JSON file : {e}'

    if error_file == False:
        print("df.columns.tolist()", df.columns.tolist())
        print("col_names_to_check", col_names_to_check)
        # Add source filename and upload the file
        if df.columns.tolist() == col_names_to_check:
            print(f"The file '{blob.name}'  is VALID")
            error_file = False 
            description = 'File is VALID'
        else:
            print(f"The file '{blob.name}' is INVALID")
            error_file = True
            description = 'Columns do not respect the columns definition'

    file_stats =  [{
        'pipeline': pipeline_name,
        'dataset': dataset_name,
        'table': table_name, 
        'source_filename': blob.name,
        'date_ingest': ingestion_date,
        'is_invalid': error_file,
        'description': description
    }]
    df_file_stats = pd.DataFrame(file_stats)

    return df, df_file_stats, error_file


def check_rows_required_column(df, required_col_list):
    """
    Check if values in columns having REQUIRED mode is NULL 
    -- > If NULL, caracteristics invalidity appended to the invalid dataset.
    
    Parameters
    ----------
    df : dataframe
    required_col_list : list of columns having mode = REQUIRED
    Result
    -------
    Returns a dataframe having lines with NULL values for columns having mode = REQUIRED
    """
    final_df_data_invalid_REASON_REQUIRED = pd.DataFrame()
    for col_required in required_col_list:
        df_data_invalid_REASON_REQUIRED = pd.DataFrame()
        df_data_invalid_REASON_REQUIRED = df[df[col_required].isna()]
        df_data_invalid_REASON_REQUIRED['invalid_reason'] = f'{col_required} is NULL whereas it is REQUIRED'
        df_data_invalid_REASON_REQUIRED['invalid_code'] = f'NULL_VALUE_NOT_EXPECTED'
        df_data_invalid_REASON_REQUIRED['invalid_col_name'] = f'{col_required}'
        df_data_invalid_REASON_REQUIRED['invalid_col_value'] = df_data_invalid_REASON_REQUIRED[col_required]
        
        final_df_data_invalid_REASON_REQUIRED = pd.concat([final_df_data_invalid_REASON_REQUIRED, df_data_invalid_REASON_REQUIRED])
    return final_df_data_invalid_REASON_REQUIRED


def check_rows_key_not_unique(df, key_col_list):
    """
    Check if tuples composed of values in columns having TRUE as Unique Identifier are not unique. 
    --> If not unique, caracteristics invalidity appended to the invalid dataset.
    
    Parameters
    ----------
    df : dataframe
    key_col_list : list of columns having key = TRUE
    Result
    -------
    Returns a dataframe having lines with a duplicated tuples of columns key 
    """
    if key_col_list == []:
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE =  pd.DataFrame()
    else :
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE = df[df.duplicated(subset=key_col_list, keep=False)]
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE_count = final_df_data_invalid_REASON_KEY_NOT_UNIQUE.groupby(key_col_list, dropna=False).size().reset_index(name='count')
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE = final_df_data_invalid_REASON_KEY_NOT_UNIQUE.merge(final_df_data_invalid_REASON_KEY_NOT_UNIQUE_count, how='left', on=key_col_list)
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE['invalid_reason'] = f'{key_col_list} is not unique : ' + final_df_data_invalid_REASON_KEY_NOT_UNIQUE['count'].astype(str) + ' occurences'
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE['invalid_code'] = f'KEY_NOT_UNIQUE'
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE['invalid_col_name'] = f'{key_col_list}'
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE['invalid_col_value'] = "['" + final_df_data_invalid_REASON_KEY_NOT_UNIQUE[key_col_list].apply(lambda row: "','".join(row.values.astype(str)), axis = 1).astype(str) + "']"
        final_df_data_invalid_REASON_KEY_NOT_UNIQUE = final_df_data_invalid_REASON_KEY_NOT_UNIQUE.drop(columns=['count'])
    return final_df_data_invalid_REASON_KEY_NOT_UNIQUE


def check_rows_parsing(df, schema):
    """
    For rows that pass the 1rst check and the 2nd check, for each column, for each column, try to parse each row into the corresponding column type.
    --> If not parsable, caracteristics invalidity appended to the invalid dataset.
    
    Parameters
    ----------
    df : dataframe
    schema : JSON : information for each column 
        - description
        - name
        - type : STRING, INTEGER, FLOAT, DATE
        - mode : NULLABLE, REQUIRED, REPEATED
        - key : TRUE or FALSE
        - date_format : %Y-%m-%d
        - float_thousand_separator : None / "," / ";" / "."
        - loat_decimal_separator : None / "," / ";" / "."
    Result
    -------
    Returns a dataframe having lines with values that are not parsable into the correct type
    """
    final_df_data_invalid_REASON_PARSING = pd.DataFrame()
    df_copy = df.copy()
    
    for col_name, col_type, col_date_format, col_float_thousand_separator, col_float_decimal_separator in [(col_info['name'],col_info['type'],col_info['date_format'],col_info['float_thousand_separator'],col_info['float_decimal_separator'])  for col_info in schema]:

        if col_type == "STRING":
            # On met null à 0 pcq après, on va filtrer sur les nulls
            df_copy[col_name] = df_copy[col_name].fillna('0')
            df_copy[col_name] = df_copy[col_name].astype(str)
        
        if (col_type == "FLOAT64") or (col_type == "FLOAT"):
            # On met null à 0 pcq après, on va filtrer sur les nulls
            df_copy[col_name] = df_copy[col_name].fillna('0')
            if col_float_thousand_separator != None:
                df_copy[col_name] = df_copy[col_name].str.replace(col_float_thousand_separator,'')
            if col_float_decimal_separator != None:
                df_copy[col_name] = df_copy[col_name].str.replace(col_float_decimal_separator,'.')
            df_copy[col_name] = pd.to_numeric(df_copy[col_name], errors='coerce', downcast="float")

        if col_type == 'INTEGER':
            # On met null à 0 pcq après, on va filtrer sur les nulls
            df_copy[col_name] = df_copy[col_name].fillna('0')
            
            # On remplace les valeurs à virgules par ERROR_COMMA (en omettant le fait que ',' est en fait '.')
            # Exemple : 10.0 n'est pas remplacé.
            # Exemple : 10.1 est remplacé.
            df_copy[col_name] = df_copy[col_name].str.replace(pat='^[0-9]*[.,][1-9]+$', repl="ERROR_COMMA", regex=True)
            # On convertit en float64.
            df_copy[col_name] = pd.to_numeric(df_copy[col_name], errors='coerce', downcast="integer")

        if (col_type == 'DATE') or (col_type=='DATETIME'):
            # On met null à 0 pcq après, on va filtrer sur les nulls
            df_copy[col_name] = df_copy[col_name].fillna('1900-01-01')
            # On convertit en date en respectant le format de date mentionné
            df_copy[col_name] = pd.to_datetime(df_copy[col_name], format=col_date_format, errors='coerce')


        # Dataframe qui contient des données non COL_TYPE pour la colonne qui doivent être du type COL_TYPE
        df_init_test_parsed_invalid = df_copy[df_copy[col_name].isna()]

        # On prend les colonnes, de sorte à identifier chaque ligne invalide.
        df_init_test_parsed_invalid = df_init_test_parsed_invalid[['index']]
        df_init_test_parsed_invalid['invalid_reason'] = f'{col_name} is not parsable to {col_type}'
        df_init_test_parsed_invalid['invalid_code'] = f'COLUMN_NOT_PARSABLE'
        df_init_test_parsed_invalid['invalid_col_name'] = f'{col_name}'

        df_init_test_parsed_invalid = df_init_test_parsed_invalid.merge(df, how='left', on=['index'])
        df_init_test_parsed_invalid['invalid_col_value'] = df_init_test_parsed_invalid[col_name]

        final_df_data_invalid_REASON_PARSING = pd.concat([final_df_data_invalid_REASON_PARSING, df_init_test_parsed_invalid])
        
    return final_df_data_invalid_REASON_PARSING


def check_rows(pipeline_name:str, dataset_name: str, table_name:str, filename:str, df, params:dict, ingestion_date:str):
    """
    Check a dataframe at level row : check_rows_required_column, check_rows_key_not_unique, check_rows_parsing
    Entries :
    - pipeline_name                    (str, required) : Name of the pipeline
    - dataset_name                     (str, required) : Name of the dataset
    - table_name                       (str, required) : Name of the table
    - filename                         (blob, required): Name of the file represented by the dataframe
    - params                           (dict, required): Parameters containing at least : schema
    - ingestion_date                   (str, required) : Ingestion date of the file
    Return :
    - final_df_rows_stats              df : stats   : pipeline, table, source_filename, # of valid lines, # of invalid lines, ingestion date
    - final_df_data_valid              df : valid   : with all the valid lines that passed the data quality checks based on the schema
    - final_df_data_invalid            df : invalid : with all the invalid lines that passed the data quality checks based on the schema
    """

    # Remove empty rows, reset index, add source_filename column, and rename columns with cleaned names
    df = df.dropna(how='all')
    df = df.reset_index()
    df = df.replace('\n', ' ', regex=True)
    df['source_filename'] = filename
    df['execution_datetime'] = ingestion_date
    
    final_df_data_invalid = pd.DataFrame()
    schema_json = params.get('schema')
    schema_df = pd.DataFrame(schema_json)

    required_columns = schema_df[schema_df["mode"]=="REQUIRED"]["name"].to_list()
    key_columns = schema_df[schema_df["unique_identifier"]==True]["name"].to_list()

    columns_name_cleaned = schema_df.name.to_list()
    columns_name_cleaned_index = ['index']
    columns_name_cleaned_index.extend(columns_name_cleaned)

    print("df.columns : ", df.columns)
    print("columns_name_cleaned_index : ", columns_name_cleaned_index)
    
    df.columns = columns_name_cleaned_index
    
    # FIRST INVALID CHECK : where value in columns with mode "REQUIRED" is NULL
    logging.info('FIRST INVALID CHECK : where value in columns with mode "REQUIRED" is NULL')
    final_df_data_invalid_REASON_REQUIRED = check_rows_required_column(df=df, required_col_list=required_columns)
    
    # SECOND INVALID CHECK : where key_columns is not unique
    logging.info('SECOND INVALID CHECK : where key_columns is not unique')
    final_df_data_invalid_REASON_KEY_NOT_UNIQUE = check_rows_key_not_unique(df=df, key_col_list=key_columns)
    
    # THIRD INVALID CHECK : where value not PARSABLE
    logging.info('THIRD INVALID CHECK : where value not PARSABLE')
    final_df_data_invalid_REASON_PARSING = check_rows_parsing(df=df, schema=schema_json)

    # Regroup invalid data REASON REQUIRED, invalid data REASON KEY NOT UNIQUE and invalid data REASON PARSING
    logging.info('Regroup invalid data REASON REQUIRED, invalid data REASON KEY NOT UNIQUE and invalid data REASON PARSING')
    final_df_data_invalid = pd.concat([final_df_data_invalid, final_df_data_invalid_REASON_REQUIRED])
    final_df_data_invalid = pd.concat([final_df_data_invalid, final_df_data_invalid_REASON_KEY_NOT_UNIQUE])
    final_df_data_invalid = pd.concat([final_df_data_invalid, final_df_data_invalid_REASON_PARSING])
    final_df_data_invalid['date_ingest'] = ingestion_date
    final_df_data_invalid['table'] = table_name
    
    full_data_invalid = final_df_data_invalid.copy()
        
    # INVALID DATASET
    logging.info('Build invalid dataset')
    final_df_data_invalid['pipeline'] = pipeline_name
    final_df_data_invalid['dataset'] = dataset_name
    final_df_data_invalid = final_df_data_invalid[['pipeline', 'dataset', 'table','source_filename','index','invalid_reason','invalid_code','invalid_col_name','invalid_col_value','date_ingest']]
    final_df_data_invalid = final_df_data_invalid.sort_values(by=['table', 'source_filename','index'])
    final_df_data_invalid = final_df_data_invalid.reset_index(drop=True)
    
    # VALID DATASET
    logging.info('Build valid dataset') 
    final_df_data_invalid_key = final_df_data_invalid.groupby(['index'], as_index=False)["invalid_reason"].count()
    final_df_data_valid = df.merge(final_df_data_invalid_key, how='left', on=['index'])
    final_df_data_valid = final_df_data_valid[final_df_data_valid['invalid_reason'].isna()]
    final_df_data_valid = final_df_data_valid.sort_values(by=['source_filename','index'])
    final_df_data_valid = final_df_data_valid.drop(columns=['index','invalid_reason'])
    
    # STATS DATASET
    if full_data_invalid.shape[0] == 0:
        nb_invalid_lines = 0
        nb_invalid_lines_NULL_VALUE_NOT_EXPECTED = 0
        nb_invalid_lines_KEY_NOT_UNIQUE = 0
        nb_invalid_lines_COLUMN_NOT_PARSABLE = 0
    else:
        nb_invalid_lines = full_data_invalid['index'].drop_duplicates().shape[0]
        nb_invalid_lines_NULL_VALUE_NOT_EXPECTED = full_data_invalid[full_data_invalid.invalid_code=='NULL_VALUE_NOT_EXPECTED']['index'].drop_duplicates().shape[0]
        nb_invalid_lines_KEY_NOT_UNIQUE = full_data_invalid[full_data_invalid.invalid_code=='KEY_NOT_UNIQUE']['index'].drop_duplicates().shape[0]
        nb_invalid_lines_COLUMN_NOT_PARSABLE = full_data_invalid[full_data_invalid.invalid_code=='COLUMN_NOT_PARSABLE']['index'].drop_duplicates().shape[0]
    
    data_invalid_stats_data =  [{
        'pipeline': pipeline_name, 
        'dataset': dataset_name,
        'table': table_name, 
        'source_filename': filename,
        'date_ingest': ingestion_date,
        'nb_valid_lines': final_df_data_valid.shape[0],
        'nb_invalid_lines': nb_invalid_lines,
        'nb_invalid_lines_NULL_VALUE_NOT_EXPECTED': nb_invalid_lines_NULL_VALUE_NOT_EXPECTED,
        'nb_invalid_lines_KEY_NOT_UNIQUE': nb_invalid_lines_KEY_NOT_UNIQUE,
        'nb_invalid_lines_COLUMN_NOT_PARSABLE': nb_invalid_lines_COLUMN_NOT_PARSABLE,   
    }]
    final_df_rows_stats = pd.DataFrame(data_invalid_stats_data)

    # CONVERT VALID DATE USING THE DEFINED FORMAT
    date_params_df = schema_df.loc[schema_df['type'] == "DATE"]
    for idx in date_params_df.index:
        col_name = date_params_df['name'][idx]
        col_date_format = date_params_df['date_format'][idx]
        final_df_data_valid[col_name] = pd.to_datetime(final_df_data_valid[col_name], format=col_date_format, errors='coerce')

    return final_df_rows_stats, final_df_data_valid, final_df_data_invalid


def quality_validation_to_gcs(
    pipeline_name:str, 
    dataset_name:str,
    table_name:str,
    core_landing_project_id:str, 
    core_landing_bucket_name:str, 
    core_landing_source_path:str, 
    core_exploitation_project_id:str, 
    core_exploitation_bucket_name:str, 
    core_exploitation_dq_stats_path:str, 
    filename_file_stats:str, 
    filename_rows_stats:str, 
    filename_data_invalid:str, 
    filename_data_valid:str,
    params:dict, 
    ts:str
    ):
    """
    Data Quality on a list of blobs : check files & check rows. Put the result in GCS, as CSV files.
    Entries 
    - pipeline_name                    (str, required) : Name of the pipeline
    - dataset_name                     (str, required) : Name of the dataset
    - table_name                       (str, required) : Name of the table
    - core_landing_project_id          (str, required): Project Id storing the files to validate
    - core_landing_bucket_name         (str, required): Bucket name storing the files to validate
    - core_landing_source_path         (str, required): Folder path storing the files to validate
    - core_exploitation_project_id     (str, required): Project Id where the files will be stored after data quality checks
    - core_exploitation_bucket_name    (str, required): Bucket name where the files will be stored after data quality checks
    - core_exploitation_dq_stats_path  (str, required): Folder path where the files will be stored after data quality checks
    - filename_file_stats              (str, required): Name of the file-stats file
    - filename_rows_stats              (str, required): Name of the rows-stats file
    - filename_data_invalid            (str, required): Name of the valid-rows file
    - filename_data_valid              (str, required): Name of the invalid-rows file
    - params                           (dict, required): Containing at least : file_info {source, name, format, separator, encoding} and schema
    - ts                               (str, required) : Execution time
    Returns
    None
    """

    core_exploitation_gcs_client = storage.Client(project=core_exploitation_project_id)
    core_exploitation_bucket = core_exploitation_gcs_client.bucket(bucket_name=core_exploitation_bucket_name)
    core_landing_gcs_client = storage.Client(project=core_landing_project_id)
    blobs = core_landing_gcs_client.list_blobs(bucket_or_name=core_landing_bucket_name, prefix=f'{core_landing_source_path}')
    df = pd.DataFrame()
    final_df_file_stats = pd.DataFrame()
    final_df_rows_stats = pd.DataFrame()
    final_df_data_valid = pd.DataFrame()
    final_df_data_invalid = pd.DataFrame()

    ts_datetime = datetime.datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')
    ingestion_date = str(ts_datetime)

    for blob in blobs:
        
        if blob.size > 0 : # Check if a file is inside the folder

            print(f'======== Analyzing : {blob.name} | Size : {blob.size} | Updated : {blob.updated} | Metadata : {blob.metadata} =======')
    
            # Get filename
            filename = blob.name

            # Check files
            print(f'=== Check file : {blob.name} ===')
            df, df_file_stats, error_file = check_files(pipeline_name=pipeline_name, dataset_name=dataset_name, table_name=table_name, blob=blob, params=params, ingestion_date=ingestion_date)
            final_df_file_stats = pd.concat([final_df_file_stats, df_file_stats])
            if error_file :
                continue
            else:
                print(f'=== Check rows : {blob.name} ===')
                rows_stats, df_data_valid, df_data_invalid = check_rows(pipeline_name=pipeline_name, dataset_name=dataset_name, table_name=table_name, filename=filename, df=df, params=params, ingestion_date=ingestion_date)
                final_df_rows_stats = pd.concat([final_df_rows_stats, rows_stats])
                final_df_data_valid = pd.concat([final_df_data_valid, df_data_valid])
                final_df_data_invalid = pd.concat([final_df_data_invalid, df_data_invalid])
                print('=== Data Quality checks done ! ===')
        else :
            continue
    
    print(f'Write file in bucket {core_exploitation_bucket_name} from project {core_exploitation_project_id}: {core_exploitation_dq_stats_path}{filename_file_stats}.csv')
    core_exploitation_bucket.blob(blob_name=f'{core_exploitation_dq_stats_path}{filename_file_stats}.csv').upload_from_string(data=final_df_file_stats.to_csv(sep=';', index=False), content_type='text/csv')

    print(f'Write file in bucket {core_exploitation_bucket_name} from project {core_exploitation_project_id}: {core_exploitation_dq_stats_path}{filename_data_valid}.csv')
    core_exploitation_bucket.blob(blob_name=f'{core_exploitation_dq_stats_path}{filename_data_valid}.csv').upload_from_string(data=final_df_data_valid.to_csv(sep=';', index=False), content_type='text/csv')

    print(f'Write file in bucket {core_exploitation_bucket_name} from project {core_exploitation_project_id}: {core_exploitation_dq_stats_path}{filename_rows_stats}.csv')
    core_exploitation_bucket.blob(blob_name=f'{core_exploitation_dq_stats_path}{filename_rows_stats}.csv').upload_from_string(data=final_df_rows_stats.to_csv(sep=';', index=False), content_type='text/csv')

    print(f'Write file in bucket {core_exploitation_bucket_name} from project {core_exploitation_project_id}: {core_exploitation_dq_stats_path}{filename_data_invalid}.csv')
    core_exploitation_bucket.blob(blob_name=f'{core_exploitation_dq_stats_path}{filename_data_invalid}.csv').upload_from_string(final_df_data_invalid.to_csv(sep=';', index=False), content_type='text/csv')
    

def quality_stats_gcs_to_bq(
    source_project_id:str,
    source_bucket_name:str,
    source_path:str,
    filename_file_stats:str,
    filename_rows_stats:str,
    filename_data_invalid:str,
    destination_project_id:str,
    destination_dataset_name:str, 
    destination_table_name_file_stats:str, 
    destination_table_name_rows_stats:str, 
    destination_table_name_data_invalid:str
    ):
    """
    Load data from GCS Bucket storing the data quality files to BQ Data Quality dataset
    Entries 
    - source_project_id                     (str, required) : Project id storing the data quality files
    - source_bucket_name                    (str, required) : Bucket name storing the data quality files
    - source_path                           (str, required) : Folder path storing the data quality files
    - filename_file_stats                   (str, required) : Name of the file-stats file
    - filename_rows_stats                   (str, required) : Name of the rows-stats file
    - filename_data_invalid                 (str, required) : Name of the valid-rows file
    - destination_project_id                (str, required) : Project id of the data quality dataset
    - destination_dataset_name              (str, required) : Data quality dataset name
    - destination_table_name_file_stats     (str, required) : Name of the file stats table
    - destination_table_name_rows_stats     (str, required) : Name of the rows stats table
    - destination_table_name_data_invalid   (str, required) : Name of the invalid data table
    Returns
    None
    """
    
    print(f'----- From project {source_project_id} and bucket {source_bucket_name}, load {source_path}{filename_file_stats}.csv to BQ {destination_project_id}.{destination_dataset_name}.{destination_table_name_file_stats} : write_mode APPEND -----')
    gcs_to_bq_load(
        source_project_id=source_project_id, 
        source_bucket_name=source_bucket_name, 
        source_path=source_path, 
        filename=filename_file_stats, 
        destination_project_id=destination_project_id,
        destination_dataset_name=destination_dataset_name, 
        destination_table_name=destination_table_name_file_stats, 
        file_info={'format':'csv'}, 
        schema=SCHEMA_TABLE_FILE_STATS, 
        write_mode='APPEND'
    )
    gcs_delete_list_blobs(project_id=source_project_id, bucket_name=source_bucket_name, source_path=source_path, file_prefix=f'{filename_file_stats}.csv')


    print(f'----- From project {source_project_id} and bucket {source_bucket_name}, load {source_path}{filename_rows_stats}.csv to BQ {destination_project_id}.{destination_dataset_name}.{destination_table_name_rows_stats} : write_mode APPEND -----')
    gcs_to_bq_load(
        source_project_id=source_project_id, 
        source_bucket_name=source_bucket_name, 
        source_path=source_path, 
        filename=filename_rows_stats, 
        destination_project_id=destination_project_id,
        destination_dataset_name=destination_dataset_name, 
        destination_table_name=destination_table_name_rows_stats, 
        file_info={'format':'csv'}, 
        schema=SCHEMA_TABLE_ROWS_STATS, 
        write_mode='APPEND'
    )
    gcs_delete_list_blobs(project_id=source_project_id, bucket_name=source_bucket_name, source_path=source_path, file_prefix=f'{filename_rows_stats}.csv')

    print(f'----- From project {source_project_id} and bucket {source_bucket_name}, load {source_path}{filename_data_invalid}.csv to BQ {destination_project_id}.{destination_dataset_name}.{destination_table_name_data_invalid} : write_mode APPEND -----')
    gcs_to_bq_load(
        source_project_id=source_project_id, 
        source_bucket_name=source_bucket_name, 
        source_path=source_path, 
        filename=filename_data_invalid, 
        destination_project_id=destination_project_id,
        destination_dataset_name=destination_dataset_name, 
        destination_table_name=destination_table_name_data_invalid, 
        file_info={'format':'csv'}, 
        schema=SCHEMA_TABLE_DATA_INVALID, 
        write_mode='APPEND'
    )
    gcs_delete_list_blobs(project_id=source_project_id, bucket_name=source_bucket_name, source_path=source_path, file_prefix=f'{filename_data_invalid}.csv')