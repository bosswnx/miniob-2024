
build: gen_parser format
	@bash build.sh

env:
	@bash build.sh init

run: build
	@./build_debug/bin/observer -f ../build_debug/etc/observer.ini -P cli

help:
	@bash build.sh -h

clean:
	@bash build.sh clean

# 由于 parser 生成的代码经常会导致 git 冲突，所以将 parser 生成的源文件放在 .gitignore 中
# 每次编译前执行该命令生成 parser 源文件
gen_parser:
	@cd src/observer/sql/parser && ./gen_parser.sh

format:
	@bash hooks/code_format.sh

.PHONY: build env run help clean gen_parser format