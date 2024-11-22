# MiniOB 2024

![USTB LOGO](https://github.com/user-attachments/assets/72936f72-9dcb-4873-896f-6d731a01eeec)

北京科技大学 我真的参加了系统内核赛

队员：[王诺贤](https://bosswnx.xyz)、[廖玮珑](https://soulter.top)、[陈渠](https://blog.virtualfuture.top/)。

初赛满分通过，全国排名第 19，北京赛区排名第 2，校排名第 1：

![image](https://github.com/user-attachments/assets/abb3adff-a905-4f54-8025-618011b4e061)

开发记录：

- [OceanBase 2024 初赛 MiniOB 开发记录](https://zhuanlan.zhihu.com/p/5953505884)
- [OceanBase 数据库内核实现赛 / 自己实现一个数据库](https://blog.soulter.top/posts/2024-oceanbase-database.html)
- [OceanBase 数据库大赛初赛结束之后](https://blog.virtualfuture.top/posts/miniob/)

## 开发规范

- 每次提交必须过编译；
- 提交前必须进行代码格式化；
- commit message 按照[规范](https://zhuanlan.zhihu.com/p/90281637)编写；
- ……

## 关于 build system

原 miniob 的构建系统包含以下功能:

- release 模式和 debug 模式的构建
- 强制消除所有编译器警告
- 开启 address sanitizer (检测指针越界、悬垂指针等内存错误)

添加了以下功能:

- 在 linux + gcc 平台开启 undefined behavior sanitizer，检测 UB（救大命了）
- 开启了 C++ 标准库的 debug 功能，插入检查代码，运行时检测 STL 容器相关的错误

参考:  

- [UndefinedBehaviorSanitizer — Clang 20.0.0git documentation](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html)  
- [AddressSanitizer · google/sanitizers Wiki](https://github.com/google/sanitizers/wiki/AddressSanitizer)  
- [Instrumentation Options (Using the GNU Compiler Collection (GCC))](https://gcc.gnu.org/onlinedocs/gcc/Instrumentation-Options.html)

**Note:** 由于 CMake 使用 file glob 添加源文件，任何新建的源文件都需要、也只需要重新执行 `cmake ..` 就能加入编译

## 一些资料

- clang 出现 `'iostream' file not found`: https://blog.csdn.net/qq_50901812/article/details/131408137

## 关于 VSCode 

前置要求：开发机器上安装 clangd，clang-tidy，clang-format

插件推荐：

- clangd 用于替代 cpptools，获得更好的代码补全和跳转
- clang-format 用于调用 clang-format 完成格式化
- git-commit-plugin 用于生成符合规范的 git commit message
   
需要设置：

- `C_Cpp.intelliSenseEngine` 设为 Disable，禁用 intelliSence，因为会和 clangd 冲突
- `C_Cpp.codeAnalysis.clangTidy.enabled` 设为 True，使用 clang-tidy 静态分析器

## 关于 clang-tidy

开启了三个 checker

- `clang-analyzer-*` clang静态分析检查器
- `google-*` 检查代码是否符合 google 的 C++代码规范
- `modernize-*` 检查代码是否符合现代 C++ 的代码规范
   
如果某个规则出现过于频繁，可以在 `.clang-tidy` 中排除，语法为 `-rule-name`  
也可以在希望屏蔽 clang-tidy 警告的行加上 `//NOLINT` 注释

参考：  
- [Clang-Tidy — Extra Clang Tools 20.0.0git documentation](https://clang.llvm.org/extra/clang-tidy/)
