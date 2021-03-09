.PHONY: bin test all fmt deploy docs server cli setup

all: fmt bin

fmt:
	-go fmt ./...

bin: cli server

cli:
	(cd ./cmd/mcbridgefs; go build)

server:
	(cd ./cmd/mcbridgefsd; go build)

deploy: deploy-cli deploy-server

deploy-cli: cli
	sudo cp cmd/mcbridgefs/mcbridgefs /usr/local/bin
	sudo chmod a+rx /usr/local/bin/mcbridgefs
	sudo cp mcbridgefs.sh /usr/local/bin
	sudo chmod a+rx /usr/local/bin/mcbridgefs.sh

deploy-server: server
	@sudo supervisorctl stop mcbridgefsd:mcbridgefsd_00
	sudo cp cmd/mcbridgefsd/mcbridgefsd /usr/local/bin
	sudo chmod a+rx /usr/local/bin/mcbridgefsd
	sudo cp operations/supervisord.d/mcbridgefsd.ini /etc/supervisord.d
	@sudo supervisorctl start mcbridgefsd:mcbridgefsd_00
