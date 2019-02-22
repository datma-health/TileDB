# Determine compiler flags for MuParserX
# Once done this will define
# MUPARSERX_FOUND - MuParserX found

#Build as external project
include(ExternalProject)
ExternalProject_Add(
  MuParserX
  DOWNLOAD_COMMAND ""
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/deps/muparserx"
  UPDATE_COMMAND ""
  PATCH_COMMAND ""
  CMAKE_ARGS ""
  INSTALL_COMMAND ""
)

ExternalProject_Add_Step(
    MuParserX Make
    COMMAND ${CMAKE_MAKE_COMMAND}
)

ExternalProject_Get_Property(MuParserX BINARY_DIR)

set(MuParserX_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/deps/muparserx/parser")
set(MuParserX_STATIC_LIBRARIES "${BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}muparserx${CMAKE_STATIC_LIBRARY_SUFFIX}")

include_directories(${MuParserX_INCLUDE_DIRS})
