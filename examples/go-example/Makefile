# EXAMPLE OF MAKEFILE

AMD64_BIN=handler_amd64
ARM64_BIN=handler_arm64

all: $(AMD64_BIN) $(ARM64_BIN)

$(AMD64_BIN):
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o $(AMD64_BIN) main.go

$(ARM64_BIN):
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o $(ARM64_BIN) main.go

zip: all
	zip -j go-function.zip $(AMD64_BIN) $(ARM64_BIN)

clean:
	rm -f $(AMD64_BIN) $(ARM64_BIN) go-function.zip
