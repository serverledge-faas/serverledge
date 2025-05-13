WASI-CC=$${HOME}/wasi-sdk/bin/clang
WASI-LD=$${HOME}/wasi-sdk/bin/wasm-ld

CFLAGS=--sysroot=$${HOME}/wasi-sdk/share/wasi-sysroot

all: 
	$(WASI-CC) $(CFLAGS) fibonacci.c -o fibonacci.wasm

clean:
	@rm -rf fibonacci.wasm


.PHONY: all clean
