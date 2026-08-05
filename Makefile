BINARY_NAME=cq
PKG=github.com/gnikyt/cq/v2

.PHONY: all build test test-coverage test-race bench docs clean

all: clean build test

build:
	go build -o ./dist/${BINARY_NAME} ${PKG}

test:
	go test -timeout 30s ${PKG}

test-coverage:
	go test -timeout 30s -coverprofile=/tmp/${BINARY_NAME}-cover ${PKG}

test-race:
	go test -timeout 30s -race ${PKG}

bench:
	go test -benchmem -bench=. -benchtime=2x ${PKG}

docs:
	cd docs/tools && go run ./gen

clean:
	go clean
	rm /tmp/${BINARY_NAME}-cover &2> /dev/null
	rm ./dist/${BINARY_NAME} &2> /dev/null
