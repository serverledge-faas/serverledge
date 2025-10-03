#!/bin/bash
../../../bin/serverledge-cli create --function sa_retrieve \
    --memory 256 \
    --runtime custom \
    --custom_image sa-sentiment-analysis-retrieve \
    --input "data_url:Text" \
    --input "local_dir:Text" \
    --input "object_name:Text" \
    --output "status:Text" \ 
    --output "local_download:Bool" \
    --output "uploaded:Bool" \
    --output "object_name:Text"


../../../bin/serverledge-cli create --function sa_extract \
    --memory 256 \
    --runtime custom \
    --custom_image sa-sentiment-analysis-extract \
    --input "tgz_input_object_name:Text" \
    --input "subset:Float" \
    --input "local_dataset_file:Text" \
    --input "local_output_dir:Text" \
    --input "output_train_object_name:Text" \
    --input "output_test_object_name:Text" \
    --output "status:Text" \
    --output "train_object_name:Text" \
    --output "test_object_name:Text" \
    --output "running_time:Float"



../../../bin/serverledge-cli create --function sa_train \
    --memory 1024 \
    --runtime custom \
    --custom_image sa-sentiment-analysis-train \
    --input "subset:Float" \
    --input "max_features:Int" \
    --input "train_object_data:Text" \
    --input "local_train_file:Text" \
    --input "local_model_file:Text" \
    --input "local_vectorizer_file:Text" \
    --input "output_model_object:Text" \
    --input "output_vectorizer_object:Text" \
    --output "status:Text" \
    --output "model_object_name:Text" \
    --output "vectorizer_object_name:Text" \
    --output "running_time:Float"



../../../bin/serverledge-cli create --function sa_evaluate \
    --memory 512 \
    --runtime custom \
    --custom_image sa-sentiment-analysis-evaluate \
    --input "test_object_data:Text" \
    --input "local_test_file:Text" \
    --input "subset:Float" \
    --input "local_model_file:Text" \
    --input "local_vectorizer_file:Text" \
    --input "input_model_object:Text" \
    --input "input_vectorizer_object:Text" \
    --output "status:Text" \
    --output "accuracy:Float" \
    --output "running_time:Float"

## TODO: How to support environment variables? 
