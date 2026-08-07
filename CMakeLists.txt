cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME fire_duck_ext)

# OpenSSL is required on native builds for HTTPS (via cpp-httplib) and service-account
# JWT signing. On WASM there are no OS sockets: HTTP is routed through DuckDB's HTTPUtil
# and OpenSSL is not used (service-account auth is unsupported there), so it is neither
# resolved nor linked. Skip resolution for clang-tidy builds which only parse source.
if (NOT CLANG_TIDY AND NOT EMSCRIPTEN)
  find_package(OpenSSL REQUIRED)
  # zlib backs CPPHTTPLIB_ZLIB_SUPPORT, which lets cpp-httplib request and
  # transparently inflate gzipped Firestore responses. Same exclusions as
  # OpenSSL: WASM routes HTTP through DuckDB's HTTPUtil (the browser handles
  # Content-Encoding itself), and clang-tidy only parses source.
  # No clash with DuckDB's vendored miniz: those symbols live in duckdb_miniz.
  find_package(ZLIB REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/fire_duck_ext_extension.cpp
    src/firestore_client.cpp
    src/firestore_auth.cpp
    src/firestore_types.cpp
    src/firestore_path_utils.cpp
    src/firestore_scanner.cpp
    src/firestore_writer.cpp
    src/firestore_secrets.cpp
    src/firestore_error.cpp
    src/firestore_logger.cpp
    src/firestore_index.cpp
    src/firestore_optimizer.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Set C++17 standard for our extension (required for std::optional)
set_target_properties(${EXTENSION_NAME} PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)
set_target_properties(${LOADABLE_EXTENSION_NAME} PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# nlohmann/json is vendored as a submodule in third_party/nlohmann_json
target_include_directories(${EXTENSION_NAME}
    PRIVATE
    third_party/nlohmann_json/include
)
target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
    third_party/nlohmann_json/include
)

# Link dependencies to both the static library and the loadable extension.
# Skip linking for clang-tidy (static analysis only) and for WASM (OpenSSL unused there).
if (NOT CLANG_TIDY AND NOT EMSCRIPTEN)
  target_link_libraries(${EXTENSION_NAME}
      OpenSSL::SSL
      OpenSSL::Crypto
      ZLIB::ZLIB
  )
  target_link_libraries(${LOADABLE_EXTENSION_NAME}
      OpenSSL::SSL
      OpenSSL::Crypto
      ZLIB::ZLIB
  )
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
