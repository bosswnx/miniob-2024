
build:
	@bash build.sh

env:
	@bash build.sh init

run: build
	@./build/bin/observer -f ../build/etc/observer.ini -P cli

.PHONY: build env run
