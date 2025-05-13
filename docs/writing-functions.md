# Writing functions

Some languages have built-in support in Serverledge and is extremely easy to
write new functions (e.g., Python, nodejs). 
For other languages, [custom container images](./custom_runtime.md) can be used to deploy and run
functions.

## Python

Available runtime: `python310` (Python 3.10)

	def handler_fun (context, params):
		return "..."

Specify the handler as `<module_name>.<function_name>` (e.g., `myfile.handler_fun`).
An example is given in `examples/hello.py`.

## NodeJS

Available runtime: `nodejs17` (NodeJS 17)

	function handler_fun (context, params) {
		return "..."
	}

	module.exports = handler_fun // this is mandatory!

Specify the handler as `<script_file_name>.js` (e.g., `myfile.js`).
An example is given in `examples/sieve.js`.

## WebAssembly/WASI

Serverledge supports the execution of WebAssembly modules through WASI using
[wasmtime-go](https://github.com/bytecodealliance/wasmtime-go).

A WebAssembly function can be defined in Serverledge by specifying the following
arguments to `serverledge-cli`:

- `runtime`: `wasi`
- `src`: the path of the `.wasm` file. If the file is larger than 2MB (`etcd`
  message size limit), it has to point to a URL of a `.tar` containing the
  `.wasm` file
- `custom_image`: the name of the `.wasm` file inside the `.tar` file

When running a function written in Python, the `handler` argument is **required**
and it has to point to the `.py` file inside the `.tar` passed in `src`.

NOTE: if the WebAssembly module is a component, the execution of the function
will be handled by the `wasmtime` CLI which has to be installed and its 
installation path added to the `PATH` environment variable.

### Examples

See `examples/wasi`.


## Custom function runtimes

Follow [these instructions](./custom_runtime.md).
