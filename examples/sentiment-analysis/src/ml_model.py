import pandas as pd
import numpy as np
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score
from pathlib import Path
import time 
import minio_client

TRAIN_DATA_FILE = "train.csv"
TRAIN_OBJECT_NAME = "data/train.csv"
TEST_DATA_FILE = "test.csv"
TEST_OBJECT_NAME = "data/test.csv"
MODEL_FILE = 'sentiment_model.pkl'
VECTORIZER_FILE = 'tfidf_vectorizer.pkl'
MODEL_OBJECT_NAME = 'model/sentiment_model.pkl'
VECTORIZER_OBJECT_NAME = 'model/tfidf_vectorizer.pkl'

def read_data_from_csv(filepath, subset = 1.0):
    try:
        df = pd.read_csv(filepath, header=None, names=['label', 'title', 'review'], encoding='utf-8')
        
        # Convert labels: 2 -> 1 (positive) and 1 -> 0 (negative) for binary classification
        df['sentiment'] = df['label'].apply(lambda x: 1 if x == 2 else 0)
        
        if subset < 1.0:
            df = df.sample(frac=subset)
        
        # The review text is the feature, the sentiment is the target
        X = df['review']
        y = df['sentiment']
        
        print(f"> Successfully read {len(df)} reviews from '{filepath}'.")
        return X, y
    
    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found. Please provide the correct path to your CSV file.")
        return None, None

def train_model(train_csv_filepath, subset = 1.0, max_features = 20000):
    X_train, y_train = read_data_from_csv(train_csv_filepath, subset=subset)
    if X_train is None:
        return (None, None)
    X_train = np.array(X_train)
    y_train = np.array(y_train)

    # Use TfidfVectorizer, which considers term frequency-inverse document frequency,
    # and n-grams to capture more context.
    vectorizer = TfidfVectorizer(ngram_range=(1, 2), max_features=max_features)
    X_train_vec = vectorizer.fit_transform(X_train)
    
    # Train a Logistic Regression model
    model = LogisticRegression(solver='liblinear')
    model.fit(X_train_vec, y_train)
    
    return (model, vectorizer)

def evaluate_model(test_csv_filepath, vectorizer, model, subset=1.0):
    X_test, y_test = read_data_from_csv(test_csv_filepath, subset=subset)
    if X_test is None:
        return 0
    X_test = np.array(X_test)
    y_test = np.array(y_test)

    # Evaluate accuracy on test set
    X_test_vec = vectorizer.transform(X_test)
    predictions = model.predict(X_test_vec)
    accuracy = accuracy_score(y_test, predictions)
    print(f"> Model Accuracy: {accuracy:.2f}")
    return accuracy


def save_model(model, vectorizer, model_filepath, vectorizer_filepath):
    # Save the trained model and the vectorizer to files
    with open(model_filepath, 'wb') as f:
        pickle.dump(model, f)
    print(f"> Model saved to '{model_filepath}'")
    
    with open(vectorizer_filepath, 'wb') as f:
        pickle.dump(vectorizer, f)
    print(f"> Vectorizer saved to '{vectorizer_filepath}'")

def load_model_and_vectorizer(model_filepath, vectorizer_filepath):
    try:
        with open(model_filepath, 'rb') as f:
            model = pickle.load(f)
        
        with open(vectorizer_filepath, 'rb') as f:
            vectorizer = pickle.load(f)
            
        print("> Model and Vectorizer loaded successfully.")
        return model, vectorizer
    
    except FileNotFoundError:
        print(f"Error: One of the files ('{model_filepath}' or '{vectorizer_filepath}') was not found. Please train and save the model first.")
        return None, None

def predict_sentiment(review, vectorizer, model):
    try:
        review_vec = vectorizer.transform([review])
        
        predicted_sentiment = model.predict(review_vec)[0]
        predicted_proba = model.predict_proba(review_vec)[0]
        sentiment_label = "Positive" if predicted_sentiment == 1 else "Negative"
        
        return {
            "sentiment": sentiment_label,
            "confidence_positive": predicted_proba[1],
            "confidence_negative": predicted_proba[0]
        }
        
    except Exception as e:
        print(f"An error occurred during prediction: {e}")
        return None



def handler_train(params, _):
    ''' Train a Sentiment Analysis model on Amazon Reviews 
    
    Parameters (all optional): 
    - train_object_data: (string) object name where the train set is stored (default: 'data/train.csv')
    - local_train_file: (string) local file used to store the train data (default: 'train.csv')
    - subset: (float) indicating the percentage of dataset to use for training the model 
    - max_features: (int) indicating the maximum number of features to use for the model 
    - local_model_file: (string) local file used to store the model before uploading it to MinIO (default: 'sentiment_model.pkl')
    - local_vectorizer_file: (string) local file used to store the vectorizer before uploading it to MinIO (default: 'tfidf_vectorizer.pkl')
    - output_model_object: (string) object name used to export the model on MinIO (default: 'model/sentiment_model.pkl')
    - output_vectorizer_object: (string) object name used to export the vectorizer on MinIO (default: 'model/tfidf_vectorizer.pkl')
    '''
    start_time = time.time()

    print("Training Sentiment Analysis model on the Amazon Review Dataset")
    try:
        subset = float(params["subset"])
    except:
        subset = 1.0

    try:
        max_features = int(params["max_features"])
    except:
        max_features = 20_000
        
    try:
        output_model_object = params["output_model_object"]
    except:
        output_model_object = MODEL_OBJECT_NAME
        
    try:
        output_vectorizer_object = params["output_vectorizer_object"]
    except:
        output_vectorizer_object = VECTORIZER_OBJECT_NAME
        
    try:
        local_model_file = params["local_model_file"]
    except:
        local_model_file = MODEL_FILE
        
    try:
        local_vectorizer_file = params["local_vectorizer_file"]
    except:
        local_vectorizer_file = VECTORIZER_FILE

    try:
        train_object_data = params["train_object_data"]
    except:
        train_object_data = TRAIN_OBJECT_NAME
    
    try:
        local_train_file = params["local_train_file"]
    except:
        local_train_file = TRAIN_DATA_FILE
    
    if minio_client.exists(output_model_object):
        print("> Model already exists!")
        running_time = time.time() - start_time
        return {"status" : "already existing", 
                "model_object_name" : output_model_object, 
                "vectorizer_object_name": output_vectorizer_object, 
                "running_time": running_time}
        
    _local_train_file = Path(local_train_file)
    if not _local_train_file.exists():
        print("> Downloading training data from MinIO")   
        ret = minio_client.download_file(train_object_data, local_train_file)
        if not ret: 
            raise Exception("Error while downloading training data.")
    else:
        print(f"> Training data already existing in: {local_train_file}")
    
    print(f"> Training model with parameters: subset={subset}, max_features={max_features} ")
    (model, vectorizer) = train_model(local_train_file, subset=subset, max_features=max_features)
    
    print(f"> Saving model locally: [{local_model_file}; {local_vectorizer_file}] ")
    save_model(model=model, vectorizer=vectorizer, model_filepath = local_model_file, vectorizer_filepath=local_vectorizer_file)

    print(f"> Uploading model to MinIO: [{output_model_object}, {output_vectorizer_object}]")
    minio_client.upload_file(local_model_file, output_model_object)
    minio_client.upload_file(local_vectorizer_file, output_vectorizer_object)
    
    running_time = time.time() - start_time
    return {"status" : "ok", 
            "model_object_name" : output_model_object,
            "vectorizer_object_name": output_vectorizer_object, 
            "running_time": running_time}
        
        
def handler_evaluate(params, _):
    ''' Evaluate a Sentiment Analysis using Amazon Reviews
    
    Parameters: 
    - test_object_data: (string) object name where the test set is stored (default: 'data/test.csv')
    - local_test_file: (string) local file used to store the test data (default: 'test.csv')
    - subset: (float) indicating the percentage of dataset to use for training the model 
    - local_model_file: (string) local file used to store the model before uploading it to MinIO (default: 'sentiment_model.pkl')
    - local_vectorizer_file: (string) local file used to store the vectorizer before uploading it to MinIO (default: 'tfidf_vectorizer.pkl')
    - input_model_object: (string) object name used to import the model from MinIO (default: 'model/sentiment_model.pkl')
    - input_vectorizer_object: (string) object name used to import the vectorizer from MinIO (default: 'model/tfidf_vectorizer.pkl')
    '''
    
    start_time = time.time()
    
    print("Evaluate Sentiment Analysis model on the Amazon Review Dataset")
    try:
        model_object = params["input_model_object"]
    except:
        model_object = MODEL_OBJECT_NAME
            
    try:
        vectorizer_object = params["input_vectorizer_object"]
    except:
        vectorizer_object = VECTORIZER_OBJECT_NAME
        
    try:
        local_model_file = params["local_model_file"]
    except:
        local_model_file = MODEL_FILE
        
    try:
        local_vectorizer_file = params["local_vectorizer_file"]
    except:
        local_vectorizer_file = VECTORIZER_FILE
        
    try:
        test_object_data = params["test_object_data"]
    except:
        test_object_data = TEST_OBJECT_NAME
        
    try:
        local_test_file = params["local_test_file"]
    except:
        local_test_file = TEST_DATA_FILE

    _local_test_file = Path(local_test_file)
    if not _local_test_file.exists():
        print(f"> Downloading testing data: {test_object_data} -> {local_test_file}")
        ret = minio_client.download_file(test_object_data, local_test_file)
        if not ret: 
            raise Exception("> Error while downloading testing data.")
    else:
        print(f"> Test data alreadly existing in: {local_test_file}")
    
    _local_model_files = Path(local_model_file)
    if not _local_model_files.exists():
        print(f"> Downloading model from MinIO: [{model_object}, {vectorizer_object}]")
        ret = minio_client.download_file(model_object, local_model_file)
        if not ret: 
            raise Exception("> Error while downloading model object.")
        
        ret = minio_client.download_file(vectorizer_object, local_vectorizer_file)
        if not ret: 
            raise Exception("> Error while downloading vectorizer object.")
    else:
        print(f"> Skipping model download from MinIO, as it already exists locally")
        
    print(f"> Loading model and vectorizer: [{local_model_file}, {local_vectorizer_file}]")
    model, vectorizer = load_model_and_vectorizer(local_model_file, local_vectorizer_file)

    try:
        subset = float(params["subset"])
    except:
        subset = 1.0
        
    print(f"> Evaluating model (parameter: subset={subset})")
    accuracy = evaluate_model(local_test_file, vectorizer, model, subset=subset)
    running_time = time.time() - start_time
    
    return {"status" : "ok", 
            "accuracy" : accuracy, 
            "running_time": running_time}