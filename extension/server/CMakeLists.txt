cmake_minimum_required(VERSION 2.8.12)

project(ServerExtension)

add_extension_definitions()

include_directories(include ../../extension/compiler/include)

add_library(
  server_extension STATIC
  server_extension.cpp common.cpp rdda_parser.cpp
  ../compiler/rdda/rdda_parse_table.cpp ../compiler/rdda/rdda_parse_view.cpp
  ../compiler/rdda/rdda_parse_select.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(
  server
  ${PARAMETERS}
  server_extension.cpp
  common.cpp
  ../compiler/rdda/rdda_parse_view.cpp
  ../compiler/rdda/rdda_parse_select.cpp
  ../compiler/rdda/rdda_parse_table.cpp
  rdda_parser.cpp)

# find_package(OpenSSL REQUIRED) target_link_libraries(httpfs_loadable_extension
# ${OPENSSL_LIBRARIES})
install(
  TARGETS server_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
