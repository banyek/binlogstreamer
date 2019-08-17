all: build
deps: 
	GOPATH=$(shell pwd) go get github.com/go-sql-driver/mysql
	GOPATH=$(shell pwd) go get github.com/koding/logging
	GOPATH=$(shell pwd) go get gopkg.in/ini.v1
build: deps
	GOPATH=$(shell pwd) go build binlogstreamer.go
compile:
	GOPATH=$(shell pwd) go build binlogstreamer.go
linux: deps
	GOPATH=$(shell pwd) GOOS=linux go build binlogstreamer.go
darwin: deps
	GOPATH=$(shell pwd) GOOS=darwin go build binlogstreamer.go
clean:
	rm -rf src/
	rm -rf pkg/
	rm -rf binlogstreamer
