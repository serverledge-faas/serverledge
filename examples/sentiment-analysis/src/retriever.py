import requests
import minio_client
from pathlib import Path

def retrieve(url, output):
    print(f" [Retrieval] Downloading dataset from {url}...")
    r = requests.get(url, stream=True)
    r.raise_for_status()
        
    output_file = Path(output)
    if not output_file.exists():    
        with open(output, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f" [Retrieval] Saved dataset to {output}")
        return True
    else:
        print(f" [Retrieval] Dataset not downloaded as it already exists in {output}")
        return False
        
def upload_to_minio(input, object_name):
    print(f" [Retrieval] Uploading file to MinIO {object_name}...")
    if minio_client.exists(object_name=object_name):
        print(f" [Retrieval] Upload not performed, object {object_name} already exists")
        return False
    else:
        minio_client.upload_file(input, object_name, override=False)
    return True

def handler(data_url, local_temp_path, object_name):
    
    if minio_client.exists(object_name):
        print("> Dataset already existing on MinIO. Retriever completes.")
        return {"status" : "already existing", 
                "local_download": False, "uploaded": False,
                "object_name" : object_name}
        
    downlaoded = retrieve(data_url, output=local_temp_path)
    uploaded = upload_to_minio(local_temp_path, object_name=object_name)
    return {"status": "ok", "local_download": downlaoded, "uploaded": uploaded, "object": object_name}
