set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_BUILD_TYPE release)

set(VCPKG_LOAD_VCVARS_ENV ON)

# MSVC 19.51 (VS 2026) miscompiles OpenSSL on ARM64 at /O2 /Gs0: the prologue
# calls __chkstk before spilling lr, so functions such as tls_parse_all_extensions
# return into their own prologue and every TLS handshake crashes
# (https://github.com/openssl/openssl/issues/26239). Build OpenSSL alone with
# clang-cl, as duckdb/duckdb-httpfs does (MIT, Stichting DuckDB Foundation).
if(PORT STREQUAL "openssl")
	set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE "${CMAKE_CURRENT_LIST_DIR}/scripts/clangcl.cmake")

	if(DEFINED VCPKG_PLATFORM_TOOLSET)
		set(VCPKG_PLATFORM_TOOLSET ClangCL)
	endif()
endif()
