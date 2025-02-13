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

**Wasm file smaller than 2MB**: assuming we have a `hello.wasm` file inside the
`/home/user/code/` directory

A function can be created using this command:

	serverledge-cli create -f func-name \
		--runtime wasi \
		--src /home/user/code/hello.wasm \
		--custom_image hello

**Python** (using the official build): assuming the `.tar` is hosted on
`localhost:8000/python.tar` and inside the `.tar` file there is a `func.py`

At the time of writing, the official build is provided by the maintainer of the
WASI platform in Python at the following [link](https://github.com/brettcannon/cpython-wasi-build/releases/tag/v3.13.0)

Serverledge assumes the structure of the `python.tar` file to be the following:

- `func.py`: this can also be in a sub-directory and is specified in the
  `handler` argument of the function creation
- `python.wasm`
- `lib/`

A function can be created using this command:

	serverledge-cli create -f func-name \
		--runtime wasi \
		--src http://localhost:8000/python.tar \
		--custom_image python \
		--handler func.py

## Custom function runtimes

Follow [these instructions](./custom_runtime.md).
