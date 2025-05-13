## C 

Download WASI SDK [here](https://github.com/WebAssembly/wasi-sdk/releases).
At the time of writing the latest release is WASI SDK 25

Update the location of the downloaded SDK in `Makefile`.

The example `fibonacci.c` is compiled to `fibonacci.wasm`:

    make

The function can be created using this command:

	serverledge-cli create -f fib-wasi \
		--runtime wasi \
		--src fibonacci.wasm \
		--custom_image fibonacci

## Python

See the `python` directory.
