#!/bin/bash
../../../bin/serverledge-cli create-workflow -s ../workflow-spec.json -f sentiment-analysis

../../../bin/serverledge-cli invoke-workflow -f sentiment-analysis \
    -p "data_url:https://matteonardelli.it/resources/amazon_review_polarity_csv2.tgz" \
    -p "local_dir:./amazon_review_polarity_csv.tgz" \
    -p "object_name:raw/amazon_review_polarity_csv.tgz" \
    -p "model:1" \
    -p "subset:1" \
    -p "max_features:10" \
    -p "train_object_data:data/train.csv" \
    -p "local_train_file:train.csv" \
    -p "local_model_file:sentiment_model.pkl" \
    -p "local_vectorizer_file:tfidf_vectorizer.pkl" \
    -p "output_model_object:model/sentiment_model.pkl" \
    -p "output_vectorizer_object:model/tfidf_vectorizer.pkl" \
    -p "test_object_data:data/test.csv" \
    -p "local_test_file:test.csv" \
    -p "local_model_file:sentiment_model.pkl" \
    -p "local_vectorizer_file:tfidf_vectorizer.pkl" \
    -p "input_model_object:model/sentiment_model.pkl" \
    -p "input_vectorizer_object:model/tfidf_vectorizer.pkl" \
    -p "tgz_input_object_name:raw/amazon_review_polarity_csv.tgz" \
    -p "local_dataset_file:./amazon_review_polarity_csv.tgz" \
    -p "local_output_dir:./data" \
    -p "output_train_object_name:data/train.csv" \
    -p "output_test_object_name:data/test.csv"
