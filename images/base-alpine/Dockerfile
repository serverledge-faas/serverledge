FROM golang:1.21-alpine AS build

WORKDIR /sedge

COPY . ./
RUN go mod download

RUN CGO_ENABLED=0 go build -o /executor ./cmd/executor/executor.go




FROM alpine:3

WORKDIR /

COPY --from=build /executor /
RUN mkdir -p /app

CMD /executor
