# Building Go Functions for Serverledge

To ensure your Go function runs correctly on Serverledge's lightweight Alpine containers (avoiding `glibc` vs `musl` issues) and supports multi-architecture clusters (x86 & ARM), follow these build steps.

## 1. Static Compilation
You must disable CGO to make the executable self-contained and compatible with any Linux distribution (including Alpine and Scratch).
You can check the example Makefile as well.

### Create executables for both architectures (run **both** commands)
```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o handler_amd64 main.go
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o handler_arm64 main.go
```
Removing debugging information is not strictly necessary, but may help producing smaller executables. 

## 2. Packaging the fat zip
```bash
zip -j function.zip handler_amd64 handler_arm64
```

## 3. Deploy to Serverledge
```bash
# create the function
bin/serverledge-cli create \
  -f gofunc \
  --memory 300 \
  --src examples/go-example/go-function.zip \
  --runtime go125

# and then execute it
bin/serverledge-cli invoke \
-f gofunc \
-p "name:Serverledge User" \
-p "num:127"

# Expected output
{
        "Success": true,
        "Result": "{\"message\":\"Hello Serverledge User\",\"number\":127,\"is_prime\":true}",
        "ResponseTime": 0.397966139,
        "IsWarmStart": false,
        "InitTime": 0.3957812,
        "QueueingTime": 0,
        "OffloadLatency": 0,
        "Duration": 0.002184848000000017,
        "Output": ""
}
```

### Or with output capture enabled
```bash
bin/serverledge-cli invoke \
-f gofunc \
-p "name:Serverledge User" \
-p "num:127" \
-o # flag to capture the output

# Expected output
{
        "Success": true,
        "Result": "{\"message\":\"Hello  Serverledge User\",\"number\":127,\"is_prime\":true}",
        "ResponseTime": 0.001020982,
        "IsWarmStart": true,
        "InitTime": 0.000054193,
        "QueueingTime": 0,
        "OffloadLatency": 0,
        "Duration": 0.000966691,
        "Output": "2025/12/01 11:12:46 Processing request for user:  Serverledge User...\n2025/12/01 11:12:46 Checked number: 127 (Prime: true)\n"
}
```
