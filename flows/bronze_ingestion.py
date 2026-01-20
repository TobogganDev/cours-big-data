from io import BytesIO
from pathlib import Path

from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SOURCES, get_minio_client

@task(name="Upload CSV to Sources Bucket", retries=2)
def upload_csv_to_sources(file_path: str, object_name: str) -> str:
    """
    Upload local CSV file to MinIO sources bucket.
    
    Args:
        file_path (str): Path to the local CSV file.
        object_name (str): Object name in the MinIO bucket.
        
    Returns:
        Object name in the MinIO bucket.
    """
    
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SOURCES):
        client.make_bucket(BUCKET_SOURCES)
        
    client.fput_object(BUCKET_SOURCES, object_name, file_path)
    print(f"Uploaded {file_path} to bucket {BUCKET_SOURCES} as {object_name}")
    return object_name

@task(name="Copy to Bronze Layer", retries=2)
def copy_to_bronze_layer(object_name: str) -> str:
    """
    Docstring for copy_to_bronze_layer
    
    :param object_name: Description
    :type object_name: str
    :return: Description
    :rtype: str
    """
    
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)
    
    response = client.get_object(BUCKET_SOURCES, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    client.put_object(BUCKET_BRONZE, object_name, BytesIO(data), length=len(data))
    
    print(f"Copied {object_name} to bucket {BUCKET_BRONZE}")
    return object_name

@flow(name="Bronze Ingestion Flow", retries=1)
def bronze_ingestion_flow(data_dir: str = "./data/sources") -> dict:
    """
    Flow to ingest data into the bronze layer.
    """
    
    data_path = Path(data_dir)
    
    clients_file = str(data_path / "clients.csv")
    achats_file = str(data_path / "achats.csv")
    
    clients_name = upload_csv_to_sources(clients_file, "clients.csv")
    achats_name = upload_csv_to_sources(achats_file, "achats.csv")
    
    bronze_clients = copy_to_bronze_layer(clients_name)
    bronze_achats = copy_to_bronze_layer(achats_name)
    
    return {
        "clients": bronze_clients,
        "achats": bronze_achats
    }
    

if __name__ == "__main__":
    result = bronze_ingestion_flow()
    print("Bronze ingestion completed:", result)