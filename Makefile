
build: format
	@bash build.sh

env:
	@bash build.sh init

run: build
	@rm -rf miniob
	@./build/bin/observer -f ./etc/observer.ini -P cli

help:
	@bash build.sh -h

clean:
	@bash build.sh clean

format:
	git clang-format --force --extensions cpp,h || true

gen_parser:
	@cd src/observer/sql/parser && ./gen_parser.sh

run_client: build
	@cd build && ./bin/obclient -s miniob.sock

.PHONY: build env run help clean gen_parser format run_client
