Amazon Review Dataset:
https://s3.amazonaws.com/fast-ai-nlp/amazon_review_polarity_csv.tgz

Source: HEFTless paper

## Requirements 

This application retrieves a dataset from AWS, stores it on [MinIO](https://github.com/minio/minio), and runs machine learning tasks on it. 

To run MinIO using docker containers, run: 

    docker run -p 9000:9000 -p 9001:9001 \                                   
        -e "MINIO_ROOT_USER=minio" \
        -e "MINIO_ROOT_PASSWORD=minio123" \
        quay.io/minio/minio server /data --console-address ":9001"

## Build the Sentiment Analysis Application

This Sentiment Analysis application on Amazon Reviews comes with a `Dockerfile`. It simplifies the application deployment. 

To build the container, run the following command:

    docker build -t sa-sentiment-analysis .

## Launch the Server 
The Sentiment Analysis application creates a HTTP Server that execute different functions according to the received REST call. 

    docker run -p 8080:8080 -ti --rm -e MINIO_ENDPOINT="172.17.0.1:9000" sa-sentiment-analysis

By default, the server listens to `8080`. The server need `MinIO` as object storage to save intermediary data. We can set information for connecting to MINIO using environment variables. 

    MINIO_ENDPOINT="172.17.0.1:9000"
    MINIO_ACCESS_KEY=minio
    MINIO_SECRET_KEY=minio123
    MINIO_BUCKET=serverledge
    MINIO_SECURE=false

### API

#### Retrieve
POST localhost:8080/invoke

    {
        "Function" : "retrieve",
        "Params" : {
            "data_url": "https://s3.amazonaws.com/fast-ai-nlp/amazon_review_polarity_csv.tgz", 
            "local_dir": "./amazon_review_polarity_csv.tgz", 
            "object_name": "raw/amazon_review_polarity_csv.tgz"
        }
    }

       
#### Extract

POST localhost:8080/invoke

    {
        "Function" : "extract",
        "Params" : {
            "tgz_input_object_name": "data/test.csv",
            "subset" : 0.002,
            "local_dataset_file": "./amazon_review_polarity_csv.tgz", 
            "local_output_dir": "./data", 
            "output_train_object_name": "data/train.csv",
            "output_test_object_name": "data/test.csv"
        }
    }


#### Train

POST localhost:8080/invoke

    {
      "Function" : "train",
      "Params" : {
          "subset": 0.001, 
          "max_features": 2, 
          "train_object_data": "data/train.csv", 
          "local_train_file": "train.csv", 
          "local_model_file": "sentiment_model.pkl", 
          "local_vectorizer_file": "tfidf_vectorizer.pkl",
          "output_model_object": "model/sentiment_model.pkl", 
          "output_vectorizer_object": "model/tfidf_vectorizer.pkl" 
      }
    }
                
#### Evaluate

POST localhost:8080/invoke

    {
        "Function" : "evaluate",
        "Params" : {
            "test_object_data": "data/test.csv", 
            "local_test_file": "test.csv", 
            "subset": 0.0002, 
            "local_model_file": "sentiment_model.pkl", 
            "local_vectorizer_file": "tfidf_vectorizer.pkl", 
            "input_model_object": "model/sentiment_model.pkl", 
            "input_vectorizer_object": "model/tfidf_vectorizer.pkl"
        }
    }
        

## Workflow

TODO: remove this section

    - retriever
    - extractor 
    - choice
      - modelHA.train -> modelHA.evaluate
      - modelLA.train -> modelLA.evaluate
  
### Serverledge Implementation 

TODO