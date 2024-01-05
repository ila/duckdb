cmake_minimum_required(VERSION 2.8.12)

project(ClientExtension)

add_extension_definitions()

include_directories(include ../server/include)

add_library(client_extension STATIC client_extension.cpp ../server/common.cpp)

set(PARAMETERS "-warnings")
build_loadable_extension(client ${PARAMETERS} client_extension.cpp
                         ../server/common.cpp)

# find_package(OpenSSL REQUIRED) target_link_libraries(httpfs_loadable_extension
# ${OPENSSL_LIBRARIES})
install(
  TARGETS client_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
