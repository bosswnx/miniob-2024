
build: format
	@bash build.sh

env:
	@bash build.sh init

run: build
	@./build_debug/bin/observer -f ../build_debug/etc/observer.ini -P cli

help:
	@bash build.sh -h

clean:
	@bash build.sh clean

format:
	@bash hooks/code_format.sh

.PHONY: build env run help clean gen_parser format