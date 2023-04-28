BINARY_NAME=cq
PKG=github.com/gnikyt/cq

all: clean build test

build:
	go build -o ./dist/${BINARY_NAME} ${PKG}

test:
	go test -timeout 30s -coverprofile=/tmp/${BINARY_NAME}-cover ${PKG}

bench:
	go test -benchmem -run=^$ -benchtime=2x ${PKG}

clean:
	go clean
	rm /tmp/${BINARY_NAME}-cover &2> /dev/null
	rm ./dist/${BINARY_NAME} &2> /dev/null
