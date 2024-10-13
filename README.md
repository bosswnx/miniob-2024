# MiniOB 2024

北京科技大学 我真的参加了系统内核赛

开发规范：

- 每次提交必须过编译；
- 提交前必须进行代码格式化；
- commit message 按照[规范](https://zhuanlan.zhihu.com/p/90281637)编写；
- ……

# 关于build system
原miniob的构建系统包含以下功能
- release模式和debug模式的构建
- 强制消除所有编译器警告
- 开启address sanitizer(检测指针越界、悬垂指针等内存错误)

添加了以下功能
- 在 linux + gcc 平台开启undefined behavior sanitizer，检测UB
- 开启了C++标准库的debug功能，插入检查代码，运行时检测STL容器相关的错误

参考:  
[UndefinedBehaviorSanitizer — Clang 20.0.0git documentation](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html)  
[AddressSanitizer · google/sanitizers Wiki](https://github.com/google/sanitizers/wiki/AddressSanitizer)  
[Instrumentation Options (Using the GNU Compiler Collection (GCC))](https://gcc.gnu.org/onlinedocs/gcc/Instrumentation-Options.html)

> **Note:** 由于 CMake 使用 file glob 添加源文件，任何新建的源文件都需要、也只需要重新执行`cmake ..`就能加入编译