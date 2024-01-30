cmake_minimum_required(VERSION 2.8.12)

project(CompilerExtension)

add_extension_definitions()

include_directories(include include/openivm include/rdda)

add_library(
        compiler_extension STATIC
        compiler_extension.cpp openivm/ivm_compile_upsert.cpp rdda/rdda_parse_table.cpp
        rdda/rdda_parse_view.cpp rdda/rdda_parse_select.cpp)

add_executable(
        compiler
        compiler_library.cpp
        compiler_extension.cpp openivm/ivm_compile_upsert.cpp rdda/rdda_parse_table.cpp
        rdda/rdda_parse_view.cpp rdda/rdda_parse_select.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(
        compiler
        ${PARAMETERS}
        compiler_library.cpp
        compiler_extension.cpp
        openivm/ivm_compile_upsert.cpp
        rdda/rdda_parse_table.cpp
        rdda/rdda_parse_view.cpp
        rdda/rdda_parse_select.cpp)

install(
        TARGETS compiler_extension
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

install(
        TARGETS compiler
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

target_link_libraries(compiler duckdb)
