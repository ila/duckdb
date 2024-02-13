cmake_minimum_required(VERSION 2.8.12)

project(IvmExtension)

add_extension_definitions()

include_directories(include ../../extension/compiler/include)

add_library(
        ivm_extension STATIC
        ivm_extension.cpp ivm_parser.cpp ivm_benchmark.cpp ivm_lineitem_benchmark.cpp ivm_groups_benchmark.cpp ivm_cross_system_benchmark.cpp ivm_cross_system_demo.cpp ivm_upsert.cpp ../compiler/data_representation.cpp ../compiler/logical_plan_to_string.cpp
        ../compiler/compiler_extension.cpp ../compiler/ivm/ivm_compile_upsert.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(
        ivm
        ${PARAMETERS}
        ivm_extension.cpp
        ivm_parser.cpp
        ivm_benchmark.cpp
        ivm_lineitem_benchmark.cpp
        ivm_groups_benchmark.cpp
        ivm_cross_system_benchmark.cpp
        ivm_cross_system_demo.cpp
        ivm_upsert.cpp
        ../compiler/data_representation.cpp
        ../compiler/logical_plan_to_string.cpp
        ../compiler/compiler_extension.cpp
        ../compiler/ivm/ivm_compile_upsert.cpp)

find_package(PostgreSQL REQUIRED)
target_link_libraries(ivm_loadable_extension PostgreSQL::PostgreSQL)
target_link_libraries(ivm_extension PostgreSQL::PostgreSQL)

install(
        TARGETS ivm_extension
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
