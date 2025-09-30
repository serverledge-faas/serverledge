import minio_client
from pathlib import Path
import os 
import tarfile
import time 
import pandas as pd

INPUT_OBJECT = "amazon_review_polarity_csv.tgz"
LOCAL_DATASET = "./amazon_review_polarity_csv.tgz"
OUTPUT_TRAINSET_OBJECT = "data/train.csv"
OUTPUT_TESTSET_OBJECT = "data/test.csv"
LOCAL_OUTPUT_DIR = "./data"

def extract_tgz(archive_path, output_dir):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Open and extract the archive
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=output_dir)
        print(f"> Extracted {len(tar.getnames())} files to '{output_dir}'")
        print(f">  . files -> [{tar.getnames()}]")
        return tar.getnames()


def sample_csv(input_file, output_file, subset=0.1, random_state=42):
    df = pd.read_csv(input_file)
    sampled_df = df.sample(n=subset if isinstance(subset, int) else None,
                           frac=subset if isinstance(subset, float) else None,
                           random_state=random_state)
    sampled_df.to_csv(output_file, index=False)
    print(f"> Sampled data saved to '{output_file}'")

def handler(params, _):
    ''' Extract the Amazon Reviews Dataset, and upload train and test data. Sample the dataset if required. 
    
    Parameters: 
    - tgz_input_object_name: (string) object name where the dataset is stored (default: 'data/test.csv')
    - subset: (float) indicating the percentage of dataset to use for training and testing 
    - local_dataset_file: (string) local file used to store the dataset (default: './amazon_review_polarity_csv.tgz')
    - local_output_dir: (string) local file used to extract the dataset (default: './data')
    - output_train_object_name: (string) object name used to save the training dataset on MinIO (default: 'data/train.csv')
    - output_test_object_name: (string) object name used to save the testing dataset on MinIO (default: 'data/test.csv')
    '''
    
    start_time = time.time()
        
    print("Extractor")
    # Getting Parameters
    try:
        tgz_input_object_name = params["tgz_input_object_name"]
    except:
        tgz_input_object_name = INPUT_OBJECT
        
    try:
        output_train_object_name = params["output_train_object_name"]
    except:
        output_train_object_name = OUTPUT_TRAINSET_OBJECT
        
    try:
        output_test_object_name = params["output_test_object_name"]
    except:
        output_test_object_name = OUTPUT_TESTSET_OBJECT
    
    try:
        local_dataset_file = params["local_dataset_file"]
    except:
        local_dataset_file = LOCAL_DATASET  
    
    try:
        local_output_dir = params["local_output_dir"]
    except:
        local_output_dir = LOCAL_OUTPUT_DIR

    try:
        subset = float(params["subset"])
    except:
        subset = 1.0
        
    # Check if final file already exists: 
    if minio_client.exists(output_train_object_name):
        print(f"> Train data already exists on MinIO: {output_train_object_name}")
        running_time = time.time() - start_time
        return {"status" : "already existing", 
                "train_object_name" : output_train_object_name, 
                "test_object_name": output_test_object_name, 
                "running_time": running_time}
        
    # Download dataset in tgz format
    _local_dataset_file = Path(local_dataset_file)
    if not _local_dataset_file.exists():
        print(f"> Downloading dataset from MinIO: {tgz_input_object_name} -> {local_dataset_file}")
        ret = minio_client.download_file(tgz_input_object_name, local_dataset_file)
        if not ret: 
            raise Exception("> Error while downloading testing data.")
    else:
        print(f"> Dataset alreadly existing in: {local_dataset_file}")
    
    # Extracting file. Expected train.csv, test.csv
    print(f"> Extracting '{local_dataset_file}' to '{local_output_dir}'")
    files = extract_tgz(local_dataset_file, local_output_dir)
    
    # Get real files (not folder or readme.txt)
    # TODO: Here we hardcode some logic, we are not interested in a robust implementation of this function. 
    prefix = files[0]
    if prefix.startswith("."):
        prefix = files[1]
        
    # Sampling
    local_train_data = f"{local_output_dir}/{prefix}/train.csv"
    local_test_data = f"{local_output_dir}/{prefix}/test.csv"
    if subset < 1.0: 
        print(f"> Sampling data: {subset}")
        sampled_train_data = f"{local_output_dir}/{prefix}/train-{subset}.csv" 
        sample_csv(local_train_data, sampled_train_data, subset=subset)
        print(f"> Train data updated")
        
        sampled_test_data = f"{local_output_dir}/{prefix}/test-{subset}.csv" 
        sample_csv(local_test_data, sampled_test_data, subset=subset)
        print(f"> Test data updated")
        
        local_train_data = sampled_train_data
        local_test_data = sampled_test_data
    
    # Upload files
    print(f"> Uploading train data to MinIO: {local_train_data} -> {output_train_object_name}]")
    minio_client.upload_file(local_train_data, output_train_object_name)

    print(f"> Uploading test data to MinIO: {local_test_data} -> {output_test_object_name}]")
    minio_client.upload_file(local_test_data, output_test_object_name)
    
    running_time = time.time() - start_time
    return {"status" : "ok", 
                "train_object_name" : output_train_object_name, 
                "test_object_name": output_test_object_name, 
                "running_time": running_time}
