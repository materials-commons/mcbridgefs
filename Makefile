.PHONY: bin test all fmt deploy docs server cli setup

all: fmt bin

fmt:
	-go fmt ./...

bin: cli server

cli:
	(cd ./cmd/mcbridgefs; go build)

server:
	(cd ./cmd/mcbridgefsd; go build)

deploy: cli server
	sudo cp cmd/mcbridgefs/mcbridgefs /usr/local/bin
	sudo cp mcbridgefs.sh /usr/local/bin
