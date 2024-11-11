from google.cloud import storage

def gcs_delete_list_blobs(project_id:str, bucket_name:str, source_path:str, file_prefix:str=None):
    """
    Remove blobs in Google Cloud Storage within a project.
    Entries :
        - project_id                  (string, required): The project identifier
        - bucket_name                 (string, required): The bucket name in the project
        - path                        (string, required): The object path in the project
        - file_prefix                 (string, optional): The object prefix in the project. If NULL, only take into account the path.
    """
    gcs_client = storage.Client(project=project_id)
    blobs_path = f'{source_path}{file_prefix}' if file_prefix else f'{source_path}'
    blobs = gcs_client.list_blobs(bucket_or_name=bucket_name, prefix=blobs_path)
    for blob in blobs:
        if blob.size > 0 : # Check if a file is inside the folder
            print(f"Delete Blob in bucket {bucket_name} from project {project_id}: {blob.name}")
            blob.delete()