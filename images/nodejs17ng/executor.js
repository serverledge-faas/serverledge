let path = require('path');
var http = require('http');

http.createServer(async (request, response) => {

	if (request.method !== 'POST') {
		response.writeHead(404);
		response.end('Invalid request method');
	} else {
		const buffers = [];

		for await (const chunk of request) {
			buffers.push(chunk);
		}

		const data = Buffer.concat(buffers).toString();
		const contentType = 'application/json';

		try {
			const reqbody = JSON.parse(data);

			var handler = reqbody["Handler"]
			var handler_dir = reqbody["HandlerDir"]
			var params = reqbody["Params"]
			var return_output = reqbody["ReturnOutput"]

			var context = {}
			if (process.env.CONTEXT !== "undefined") {
				context = process.env.CONTEXT
			}

			let h = require(path.join(handler_dir, handler))

			result = h(params, context)

			resp = {}
			resp["Result"] = JSON.stringify(result);
			resp["Success"] = true
			if (return_output === true) {
				resp["Output"] = "Output capture not supported for this runtime yet."
			} else {
				resp["Output"] = ""
			}


			response.writeHead(200, { 'Content-Type': contentType });
			response.end(JSON.stringify(resp), 'utf-8');
		} catch (error) {
			resp = {}
			resp["Success"] = false
			resp["Output"] = "Output capture not supported for this runtime yet."
			response.writeHead(500, { 'Content-Type': contentType });
			response.end(JSON.stringify(resp), 'utf-8');
		}
	}

}).listen(8080);
console.log('Server running');



