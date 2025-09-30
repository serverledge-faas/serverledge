#!/bin/bash

cd ../src
docker build --build-arg HANDLER_ENV="retrieve" -t sa-sentiment-analysis-retrieve .      
docker build --build-arg HANDLER_ENV="extract" -t sa-sentiment-analysis-extract .      
docker build --build-arg HANDLER_ENV="train" -t sa-sentiment-analysis-train .      
docker build --build-arg HANDLER_ENV="evaluate" -t sa-sentiment-analysis-evaluate .      
