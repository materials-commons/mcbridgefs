.PHONY: bin test all fmt deploy docs server cli setup

all: fmt bin

fmt:
	-go fmt ./...

bin: cli

cli:
	(cd ./cmd/mcbridgefs; go build)

deploy: cli
	sudo cp cmd/mcbridgefs/mcbridgefs /usr/local/bin
	sudo cp mcbridgefs.sh /usr/local/bin
