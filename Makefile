.PHONY: all deps build linux darwin test clean

all: build

deps:
	go mod download

build:
	go build .

linux:
	GOOS=linux go build .

darwin:
	GOOS=darwin go build .

test:
	go test ./...

clean:
	rm -f binlogstreamer
