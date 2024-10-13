# MiniOB 2024

北京科技大学 我真的参加了系统内核赛

开发规范：

- 每次提交必须过编译；
- 提交前必须进行代码格式化；
- commit message 按照[规范](https://zhuanlan.zhihu.com/p/90281637)编写；
- ……

# 关于build system

原 miniob 的构建系统包含以下功能:

- release 模式和 debug 模式的构建
- 强制消除所有编译器警告
- 开启 address sanitizer (检测指针越界、悬垂指针等内存错误)

添加了以下功能:

- 在 linux + gcc 平台开启 undefined behavior sanitizer，检测 UB
- 开启了 C++ 标准库的 debug 功能，插入检查代码，运行时检测 STL 容器相关的错误

参考:  

- [UndefinedBehaviorSanitizer — Clang 20.0.0git documentation](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html)  
- [AddressSanitizer · google/sanitizers Wiki](https://github.com/google/sanitizers/wiki/AddressSanitizer)  
- [Instrumentation Options (Using the GNU Compiler Collection (GCC))](https://gcc.gnu.org/onlinedocs/gcc/Instrumentation-Options.html)

**Note:** 由于 CMake 使用 file glob 添加源文件，任何新建的源文件都需要、也只需要重新执行 `cmake ..` 就能加入编译

## 一些资料

- clang 出现 `'iostream' file not found`: https://blog.csdn.net/qq_50901812/article/details/131408137