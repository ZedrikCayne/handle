cmake_minimum_required(VERSION 3.24)

project( handle )

set(CMAKE_COMPILE_WARNING_AS_ERROR ON)

if ( ${CMAKE_SYSTEM_NAME} MATCHES "Darwin" )
set(MACOSX TRUE)
find_package(OpenSSL)
else()
endif()

set( SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src )

set(CMAKE_BUILD_TYPE Debug)

file( GLOB SOURCES ${SOURCE_DIR}/*.cpp ${SOURCE_DIR}/*.c )

add_executable( handle ${SOURCES} )

target_include_directories( handle PUBLIC crankshaft/include )

target_link_directories( handle PUBLIC crankshaft/lib )

if( MACOSX )
if( OPENSSL_FOUND )
target_include_directories( handle PUBLIC ${OPENSSL_INCLUDE_DIR} )
target_link_libraries( handle ${OPENSSL_LIBRARIES} )
target_link_libraries( handle -lcrankshaft )
endif()
else()
target_link_libraries( handle m ssl crypto pthread m z dl crankshaft )
endif()
