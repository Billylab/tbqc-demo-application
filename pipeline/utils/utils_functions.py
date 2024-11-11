from google.cloud import storage
import pandas as pd
import io


def excel_file_to_csv_string(project_id:str, bucket_name:str, source_path:str, file_info:dict, filename:str) -> str:
    """
    Convert excel file into csv string.
    Entries :
        - project_id             (string, required): The project identifier         
        - bucket_name            (string, required): The bucket name
        - source_path            (string, required): The source object path
        - file_info              (dict, required): The excel file information
        - filename               (string, required): The filename to convert
    """
    # Source - Initialization - Client & Bucket
    gcs_client = storage.Client(project=project_id)
    bucket = gcs_client.bucket(bucket_name=bucket_name)
    # File information - Initialization
    sheet_name = file_info.get('sheet_name')
    data_columns = file_info.get('data_columns')
    data_from_rows = file_info.get('data_from_rows')
    # Get the excel file blob
    blob = bucket.blob(name=f'{source_path}{filename}.xlsx')
    excel_content = blob.download_as_bytes()

    df = pd.read_excel(io=excel_content, sheet_name=sheet_name, engine='openpyxl', usecols=data_columns, skiprows=data_from_rows, dtype=str)
    df = df.dropna(how='all')
    df.columns = df.columns.str.replace("\n", " ")
    csv_content = df.to_csv(path_or_buf=None, sep=";", index=False, header=True)

    return csv_content
    

def read_file_csv_length(blob:str, separator:str, encoding:str) -> int:
    """
    Get the number of rows in a dataframe
    Entries :
    - blob                             (blob, required): Blob object
    - encoding                         (str, required): The csv blob encoding
    Return :
    - length                           Number of rows
    """
    df = pd.DataFrame()
    length = 0    

    csv_content = blob.download_as_string()

    try:
        df = pd.read_csv(filepath_or_buffer=io.BytesIO(csv_content), dtype=str, encoding=encoding, sep=separator)
        length = len(df)
    except: 
        print(f"File {blob.name} is empty or has 0 line.")

    return length


