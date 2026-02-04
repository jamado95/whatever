.PHONY: build run test lint fmt tidy check

build:
	go build -o whatever ./cmd

run:
	go run ./cmd/main.go

test:
	go test ./...

fmt:
	gofmt -w .

lint:
	golangci-lint run

lint-clean:
	golangci-lint cache clean && golangci-lint run

tidy:
	go mod tidy

check: fmt tidy test lint