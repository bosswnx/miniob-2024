execute_process(
        COMMAND clang-format --version
        RESULT_VARIABLE clang_format_exitcode
)
if (NOT clang_format_exitcode EQUAL 0)
    message(WARNING "clang-format is not in your PATH")
endif ()

file(COPY ${CMAKE_SOURCE_DIR}/hooks/pre-commit DESTINATION ${CMAKE_SOURCE_DIR}/.git/hooks/)
message("git hooks installed!")