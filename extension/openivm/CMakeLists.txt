cmake_minimum_required(VERSION 2.8.12)

project(OpenivmExtension)

add_extension_definitions()

find_package(PostgreSQL REQUIRED)

include_directories(include ../../extension/compiler/include ../../extension/postgres_scanner/include)

add_library(
        openivm_extension STATIC
        openivm_extension.cpp openivm_parser.cpp openivm_benchmark.cpp openivm_lineitem_benchmark.cpp openivm_groups_benchmark.cpp openivm_cross_system_benchmark.cpp openivm_cross_system_demo.cpp openivm_upsert.cpp ../compiler/logical_plan_to_string.cpp
        ../compiler/compiler_extension.cpp ../compiler/openivm/openivm_compile_upsert.cpp
)

set(PARAMETERS "-warnings")
build_loadable_extension(
        openivm
        ${PARAMETERS}
        openivm_extension.cpp
        openivm_parser.cpp
        openivm_benchmark.cpp
        openivm_lineitem_benchmark.cpp
        openivm_groups_benchmark.cpp
        openivm_cross_system_benchmark.cpp
        openivm_cross_system_demo.cpp
        openivm_upsert.cpp
        ../compiler/logical_plan_to_string.cpp
        ../compiler/compiler_extension.cpp
        ../compiler/openivm/openivm_compile_upsert.cpp
        )

target_link_libraries(openivm_loadable_extension PostgreSQL::PostgreSQL)
target_link_libraries(openivm_extension PostgreSQL::PostgreSQL)

install(
        TARGETS openivm_extension
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
