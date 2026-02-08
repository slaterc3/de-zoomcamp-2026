import os
import requests
from google.cloud import storage

# YOUR SPECIFIC SETTINGS
BUCKET_NAME = "kestra-zoomcamp-slater-2026" 

def upload_to_gcs(bucket_name, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def download_and_upload():
    # Only Jan-Jun 2024 as per HW requirements
    months = ["01", "02", "03", "04", "05", "06"]
    for month in months:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{month}.parquet"
        file_name = f"yellow_tripdata_2024-{month}.parquet"
        
        print(f"Downloading {file_name}...")
        response = requests.get(url)
        if response.status_code == 200:
            with open(file_name, 'wb') as f:
                f.write(response.content)
            
            print(f"Uploading {file_name} to GCS...")
            upload_to_gcs(BUCKET_NAME, file_name, file_name)
            
            os.remove(file_name) # Cleanup local storage
            print(f"Finished {month}")
        else:
            print(f"Failed {month}: {response.status_code}")

if __name__ == "__main__":
    download_and_upload()