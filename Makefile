.PHONY: build test chaos bench proto clean docker

BINARY=raftnova
PROTO_DIR=pkg/proto

build:
	go build -o bin/$(BINARY) ./cmd/server

test:
	go test -v -count=1 ./internal/...

chaos:
	go test -v -timeout 120s -count=1 ./tests/chaos/...

bench:
	go test -bench=. -benchmem ./tests/chaos/...

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/raft.proto

clean:
	rm -rf bin/ data/
	go clean -testcache

docker:
	docker-compose up --build -d

docker-down:
	docker-compose down -v

vet:
	go vet ./...

lint:
	golangci-lint run ./...

all: proto build test
