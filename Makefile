
build: format
	@bash build.sh

env:
	@bash build.sh init

run_debug: build
	@rm -rf miniob
	@./build_debug/bin/observer -f ./etc/observer.ini -P cli

run_release: build
	@rm -rf miniob
	@./build/bin/observer -f ./etc/observer.ini -P cli

help:
	@bash build.sh -h

clean:
	@bash build.sh clean

format:
	git clang-format --force --extensions cpp,h || true

.PHONY: build env run help clean gen_parser format