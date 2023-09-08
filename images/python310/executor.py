# Python 3 server example
from http.server import BaseHTTPRequestHandler, HTTPServer
import time
import os
import sys
import importlib
import json

hostName = "0.0.0.0"
serverPort = 8080

#executed_modules = {}
added_dirs = {}

from io import StringIO
import sys

class CaptureOutput:
    def __enter__(self):
        self._stdout_output = ''
        self._stderr_output = ''

        self._stdout = sys.stdout
        sys.stdout = StringIO()

        self._stderr = sys.stderr
        sys.stderr = StringIO()

        return self

    def __exit__(self, *args):
        self._stdout_output = sys.stdout.getvalue()
        sys.stdout = self._stdout

        self._stderr_output = sys.stderr.getvalue()
        sys.stderr = self._stderr

    def get_stdout(self):
        return self._stdout_output

    def get_stderr(self):
        return self._stderr_output

class Executor(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        request = json.loads(post_data.decode('utf-8'))

        if not "invoke" in self.path:
            self.send_response(404)
            self.end_headers()
            return

        handler = request["Handler"]
        handler_dir = request["HandlerDir"]

        try:
            params = request["Params"]
        except:
            params = {}

        if "context" in os.environ:
            context = json.loads(os.environ["CONTEXT"])
        else:
            context = {}

        if not handler_dir in added_dirs:
            sys.path.insert(1, handler_dir)
            added_dirs[handler_dir] = True

        # Get module name
        module,func_name = os.path.splitext(handler)
        func_name = func_name[1:] # strip initial dot
        loaded_mod = None

        return_output = bool(request["ReturnOutput"])

        response = {}

        try:
            # Call function
            if loaded_mod is None:
                loaded_mod = importlib.import_module(module)

            if not return_output:
                result = getattr(loaded_mod, func_name)(params, context)
                response["Output"] = ""
            else:
                with CaptureOutput() as capturer:
                    result = getattr(loaded_mod, func_name)(params, context)
                response["Output"] = str(capturer.get_stdout()) + "\n" + str(capturer.get_stderr())

            response["Result"] = json.dumps(result)
            response["Success"] = True
        except Exception as e:
            print(e, file=sys.stderr)
            response["Success"] = False

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(bytes(json.dumps(response), "utf-8"))



if __name__ == "__main__":
    webServer = HTTPServer((hostName, serverPort), Executor)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")

