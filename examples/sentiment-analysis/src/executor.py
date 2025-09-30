from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import json
import retriever
import ml_model
import extractor

hostName = "0.0.0.0"
serverPort = 8080

DATA_URL = os.getenv("DATA_URL", "https://s3.amazonaws.com/fast-ai-nlp/amazon_review_polarity_csv.tgz")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./amazon_review_polarity_csv.tgz")
OBJECT_NAME = os.getenv("OBJECT_NAME", "raw/amazon_review_polarity_csv.tgz")

HANDLER_ENV = os.getenv("HANDLER_ENV")

class Executor(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = 0
        post_data = None
        request = { }
        raw_content_length = self.headers['Content-Length']
        if raw_content_length:
            content_length = int(raw_content_length)
            post_data = self.rfile.read(content_length)
            print(f" POST: Received post_data: {post_data}")
            request = json.loads(post_data.decode('utf-8'))

        if not "invoke" in self.path:
            self.send_response(404)
            self.end_headers()
            return

        try:
            params = request["Params"]
        except:
            params = {} 
        try:
            func = request["Function"]
        except:
            func = None

        if "context" in os.environ:
            context = json.loads(os.environ["CONTEXT"]) 
        else:
            context = {}

        response = {}
        try:
            if func is None and HANDLER_ENV is None:
                raise Exception("function not defined!")
            
            if func == "retrieve" or HANDLER_ENV.lower() == "retrieve":
                ''' Invocation example: 
                
                    POST localhost:8080/invoke
                    {
                        "Function" : "retrieve",
                        "Params" : {
                            "data_url": "https://s3.amazonaws.com/fast-ai-nlp/amazon_review_polarity_csv.tgz", 
                            "local_dir": "./amazon_review_polarity_csv.tgz", 
                            "object_name": "raw/amazon_review_polarity_csv.tgz"
                        }
                    }
                '''
                data_url = str(params.get("data_url", DATA_URL))
                local_temp_dir = str(params.get("local_dir", OUTPUT_PATH))
                data_object_name = str(params.get("object_name", OBJECT_NAME))
                
                print(f"Running function 'retriever' with params {data_url}, {local_temp_dir}, {data_object_name}")
                result = retriever.handler(data_url=data_url, local_temp_path=local_temp_dir, object_name=data_object_name)

            elif func == "train" or HANDLER_ENV.lower() == "train":
                ''' Invocation example: 
                
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
                '''
                print(f"Running function 'train' with params {params}")
                result = ml_model.handler_train(params, context)

            elif func == "evaluate" or HANDLER_ENV.lower() == "evaluate":
                ''' Invocation example: 
                
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
                '''
                print(f"Running function 'handle_evaluate' with params {params}, {context}")
                result = ml_model.handler_evaluate(params, context)
            
            elif func == "extract" or HANDLER_ENV.lower() == "extract": 
                ''' Invocation example: 
                
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
                '''
                print(f"Running function 'extract' with params {params}, {context}")
                result = extractor.handler(params, context)

            else:
                raise Exception("Unsupported function")
              
            response["Result"] = json.dumps(result)
            response["Success"] = True
        except Exception as e:
            print(e)
            response["Success"] = False
            response["Error"] = str(e)

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(bytes(json.dumps(response), "utf-8"))



if __name__ == "__main__":      
    print("Launching HTTP Server... ")  
    srv = HTTPServer((hostName, serverPort), Executor)
    try:
        print("Running server ... ")
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    srv.server_close()

