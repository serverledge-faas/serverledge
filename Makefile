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

proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/scheduling/protobuf/solver.proto

DOCKERHUB_USER=grussorusso
images:  image-python310 image-nodejs17ng image-base
image-python310:
	docker build -t $(DOCKERHUB_USER)/serverledge-python310 -f images/python310/Dockerfile .
image-base:
	docker build -t $(DOCKERHUB_USER)/serverledge-base -f images/base-alpine/Dockerfile .
image-nodejs17ng:
	docker build -t $(DOCKERHUB_USER)/serverledge-nodejs17ng -f images/nodejs17ng/Dockerfile .

images-multi-arch:  image-python310-multi-arch image-nodejs17ng-multi-arch image-base-multi-arch
PLATFORMS=linux/amd64,linux/arm64,linux/arm/v7
image-python310-multi-arch:
	docker buildx build --platform $(PLATFORMS) -t $(DOCKERHUB_USER)/serverledge-python310 -f images/python310/Dockerfile --push .
image-base-multi-arch:
	docker buildx build --platform $(PLATFORMS) -t $(DOCKERHUB_USER)/serverledge-base -f images/base-alpine/Dockerfile --push .
image-nodejs17ng-multi-arch:
	docker buildx build --platform $(PLATFORMS) -t $(DOCKERHUB_USER)/serverledge-nodejs17ng -f images/nodejs17ng/Dockerfile --push .

push-images:
	docker push $(DOCKERHUB_USER)/serverledge-python310
	docker push $(DOCKERHUB_USER)/serverledge-base
	docker push $(DOCKERHUB_USER)/serverledge-nodejs17ng

unit_test:
	go test -v ./...

test:
	INTEGRATION=1 go test -v ./...

# to get coverage, install cover
# go get golang.org/x/tools/cmd/cover
test_coverage:
	go test -coverprofile coverage.txt ./...

coverage:
	go tool cover -html=coverage.txt

.PHONY: serverledge serverledge-cli lb executor test images

