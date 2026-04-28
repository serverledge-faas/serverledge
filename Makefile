BIN=bin
GO=go
all: serverledge executor serverledge-cli lb

serverledge:
	$(GO) build -o $(BIN)/$@ cmd/$@/main.go

lb:
	CGO_ENABLED=0 $(GO) build -o $(BIN)/$@ cmd/$@/main.go

serverledge-cli:
	CGO_ENABLED=0 $(GO) build -o $(BIN)/$@ cmd/cli/main.go

executor:
	CGO_ENABLED=0 $(GO) build -o $(BIN)/$@ cmd/$@/executor.go

DOCKERHUB_USER=fmuschera
images:  image-python310 image-nodejs17ng image-base
image-python310:
	docker build -t $(DOCKERHUB_USER)/serverledge-python310 -f images/python310/Dockerfile .
image-base:
	docker build -t $(DOCKERHUB_USER)/serverledge-base -f images/base-alpine/Dockerfile .
image-nodejs17ng:
	docker build -t $(DOCKERHUB_USER)/serverledge-nodejs17ng -f images/nodejs17ng/Dockerfile .

push-images:
	docker push $(DOCKERHUB_USER)/serverledge-python310
	docker push $(DOCKERHUB_USER)/serverledge-base
	docker push $(DOCKERHUB_USER)/serverledge-nodejs17ng

# Runs integration tests (all tests EXCEPT unit tests)
test:
	$(GO) test -v $(shell $(GO) list ./... | grep -Ev 'internal/container|examples')

# Runs only unit tests
unit-test:
	go test -v -short ./internal/container/... ./internal/lb/...

.PHONY: serverledge serverledge-cli lb executor test unit-test integration-test images

clean:
	@test -n "$(BIN)" && [ -d "$(BIN)" ] && rm -rf $(BIN) || { echo "Invalid BIN directory: $(BIN)"; exit 1; } && go clean -testcache

