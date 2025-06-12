cmake_minimum_required(VERSION 3.5)

project(Differential_privacyExtension)

include_directories(include)

set(DIFFERENTIAL_PRIVACY_EXTENSION_FILES differential_privacy_extension.cpp
                                         dp_parser.cpp)

build_static_extension(differential_privacy
                       ${DIFFERENTIAL_PRIVACY_EXTENSION_FILES})
build_loadable_extension(differential_privacy " "
                         ${DIFFERENTIAL_PRIVACY_EXTENSION_FILES})

install(
  TARGETS differential_privacy_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
