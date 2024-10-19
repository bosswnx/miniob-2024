
build: format
	@bash build.sh

env:
	@bash build.sh init

run: build
	@rm -rf miniob
	@./build_debug/bin/observer -f ../build_debug/etc/observer.ini -P cli

help:
	@bash build.sh -h

clean:
	@bash build.sh clean

format:
	git clang-format --force --extensions cpp,h || true

.PHONY: build env run help clean gen_parser format