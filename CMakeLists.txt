cmake_minimum_required(VERSION 3.12.0)
project(AsterHiredis)

macro(getVersionBit name)
  set(VERSION_REGEX "^#define ${name} (.+)$")
  file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/AsterHiredis.hh" VERSION_BIT
       REGEX ${VERSION_REGEX})
  string(REGEX REPLACE ${VERSION_REGEX} "\\1" ${name} "${VERSION_BIT}")
endmacro(getVersionBit)

getversionbit(ASTER_HIREDIS_MAJOR)
getversionbit(ASTER_HIREDIS_MINOR)
getversionbit(ASTER_HIREDIS_PATCH)
getversionbit(ASTER_HIREDIS_SONAME)
set(VERSION
    "${ASTER_HIREDIS_MAJOR}.${ASTER_HIREDIS_MINOR}.${ASTER_HIREDIS_PATCH}")
message("Detected version: ${VERSION}")

project(AsterHiredis VERSION "${VERSION}")
include(GNUInstallDirs)

set(CMAKE_C_STANDARD 99)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_DEBUG_POSTFIX _debug)

set(aster_hiredis_sources
    standalone.cc
    sentinel.cc
    cluster.cc
    shards.cc
    connection.cc
    reader.cc
    writer.cc
    reply.cc
    alloc.cc
    sds.cc)

set(aster_hiredis_sources ${aster_hiredis_sources})

if(NOT DPDK_DIR)
  set(DPDK_DIR /usr/local/stow/dpdk)
endif()
set(CMAKE_PREFIX_PATH ${DPDK_DIR} ${CMAKE_PREFIX_PATH})

find_package(Seastar REQUIRED)

# AsterHiredis requires atleast C++17, recommend C++20
add_library(aster_hiredis SHARED ${aster_hiredis_sources})
add_library(aster_hiredis_static STATIC ${aster_hiredis_sources})

target_link_libraries(aster_hiredis Seastar::seastar)
target_link_libraries(aster_hiredis_static Seastar::seastar)

set_target_properties(aster_hiredis PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS TRUE
                                               VERSION "${HIREDIS_SONAME}")
set_target_properties(aster_hiredis_static PROPERTIES COMPILE_PDB_NAME
                                                      aster_hiredis_static)
set_target_properties(
  aster_hiredis_static PROPERTIES COMPILE_PDB_NAME_DEBUG
                                  aster_hiredis_static${CMAKE_DEBUG_POSTFIX})
if(WIN32 OR MINGW)
  target_link_libraries(aster_hiredis PUBLIC ws2_32 crypt32)
  target_link_libraries(aster_hiredis_static PUBLIC ws2_32 crypt32)
endif()

target_include_directories(
  aster_hiredis PUBLIC $<INSTALL_INTERFACE:include>
                       $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
target_include_directories(
  aster_hiredis_static PUBLIC $<INSTALL_INTERFACE:include>
                              $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

configure_file(aster_hiredis.pc.in aster_hiredis.pc @ONLY)

set(CPACK_PACKAGE_VENDOR "MofHeka")
set(CPACK_PACKAGE_DESCRIPTION "A redis client base on Seastar architecture.")
set(CPACK_PROJECT_NAME ${PROJECT_NAME})
set(CPACK_PROJECT_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/MoFHeka/AsterHiredis")
set(CPACK_PACKAGE_CONTACT "mofhejia@163.com")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_RPM_PACKAGE_AUTOREQPROV ON)

include(CPack)

install(
  TARGETS aster_hiredis aster_hiredis_static
  EXPORT aster-hiredis-targets
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

if(MSVC)
  install(
    FILES $<TARGET_PDB_FILE:aster_hiredis>
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    CONFIGURATIONS Debug RelWithDebInfo)
  install(
    FILES
      $<TARGET_FILE_DIR:aster_hiredis_static>/$<TARGET_FILE_BASE_NAME:aster_hiredis_static>.pdb
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    CONFIGURATIONS Debug RelWithDebInfo)
endif()

# For NuGet packages
install(FILES aster_hiredis.targets DESTINATION build/native)

install(
  FILES alloc.hh
        AsterHiredis.hh
        cluster.hh
        connection.hh
        reader.hh
        reply.hh
        sds.hh
        sentinel.hh
        shards.hh
        standalone.hh
        writer.hh
        client_util.hpp
        crc16.hpp
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/aster_hiredis)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/aster_hiredis.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

export(
  EXPORT aster-hiredis-targets
  FILE "${CMAKE_CURRENT_BINARY_DIR}/hiredis-targets.cmake"
  NAMESPACE AsterHiredis::)

set(CMAKE_CONF_INSTALL_DIR share/aster_hiredis)
set(INCLUDE_INSTALL_DIR include)
include(CMakePackageConfigHelpers)
configure_package_config_file(
  aster-hiredis-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/aster-hiredis-config.cmake
  INSTALL_DESTINATION ${CMAKE_CONF_INSTALL_DIR}
  PATH_VARS INCLUDE_INSTALL_DIR)

install(
  EXPORT aster-hiredis-targets
  FILE aster-hiredis-targets.cmake
  NAMESPACE AsterHiredis::
  DESTINATION ${CMAKE_CONF_INSTALL_DIR})

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/aster-hiredis-config.cmake
        DESTINATION ${CMAKE_CONF_INSTALL_DIR})
