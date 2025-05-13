
**Python** (using the official build): assuming the `.tar` is hosted on
`localhost:8000/python.tar` and inside the `.tar` file there is a `sieve.py`

At the time of writing, the official build is provided by the maintainer of the
WASI platform in Python at the following [link](https://github.com/brettcannon/cpython-wasi-build/releases/tag/v3.13.0)

Serverledge assumes the structure of the `python.tar` file to be the following:

- `sieve.py`: the main source file; the name must be specified in the `handler` argument of the function creation
- `python.wasm` (obtained through the link above)
- `lib/` (obtained through the link above)

A function can be created using this command:

	serverledge-cli create -f func-name \
		--runtime wasi \
		--src http://localhost:8000/python.tar \
		--custom_image python \
		--handler sieve.py
