cmake_minimum_required(VERSION 3.16)

project(ariabc_pg_app LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Pull NuRaft as a library dependency without building its examples/tests.
if(NOT TARGET NuRaft::static_lib)
    set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
    add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/../NuRaft ${CMAKE_CURRENT_BINARY_DIR}/nuraft-build)
endif()

find_path(LIBPQ_INCLUDE_DIR
          NAMES libpq-fe.h
          HINTS
            ${CMAKE_CURRENT_LIST_DIR}/../src/interfaces/libpq
            /usr/include
            /usr/local/include
            /usr/include/postgresql
            /usr/local/include/postgresql)

find_path(POSTGRES_INCLUDE_DIR
          NAMES postgres_ext.h
          HINTS
            ${CMAKE_CURRENT_LIST_DIR}/../src/include
            /usr/include/postgresql
            /usr/local/include/postgresql
            /usr/include
            /usr/local/include)

find_library(LIBPQ_LIBRARY
             NAMES pq libpq
             HINTS
               ${CMAKE_CURRENT_LIST_DIR}/../src/interfaces/libpq
               /usr/lib
               /usr/local/lib
               /usr/lib/x86_64-linux-gnu
               /usr/local/lib/x86_64-linux-gnu)

find_path(RDKAFKA_INCLUDE_DIR
          NAMES librdkafka/rdkafka.h
          HINTS
            /usr/include
            /usr/local/include)

find_library(RDKAFKA_LIBRARY
             NAMES rdkafka
             HINTS
               /usr/lib
               /usr/local/lib
               /usr/lib/x86_64-linux-gnu
               /usr/local/lib/x86_64-linux-gnu)

option(KAFKA_OPTIONAL "Allow building without librdkafka (disables Kafka result publishing)" OFF)

if(NOT RDKAFKA_INCLUDE_DIR OR NOT RDKAFKA_LIBRARY)
    if(KAFKA_OPTIONAL)
        message(WARNING "librdkafka not found — building without Kafka support (KAFKA_OPTIONAL=ON).")
        set(RDKAFKA_INCLUDE_DIR "")
        set(RDKAFKA_LIBRARY "")
        set(ARIABC_PG_KAFKA_LIBS "")
        set(ARIABC_HAVE_KAFKA OFF)
    else()
        message(FATAL_ERROR "librdkafka is required. Install librdkafka development headers and library, or pass -DKAFKA_OPTIONAL=ON to build without Kafka.")
    endif()
else()
    set(ARIABC_HAVE_KAFKA ON)
    set(ARIABC_PG_KAFKA_LIBS ${RDKAFKA_LIBRARY})
endif()

set(ARIABC_COMMON_SRC
    src/logger.cc
    src/in_memory_log_store.cxx)

set(ARIABC_COMMON_INC
    ${CMAKE_CURRENT_LIST_DIR}/src
    ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/include
    ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/include/libnuraft
    ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/src)

# NuRaft examples traditionally build with standalone ASIO unless BOOST_ASIO=ON.
set(ARIABC_TRANSPORT_DEFS ASIO_STANDALONE)
if(BOOST_ASIO)
    set(ARIABC_TRANSPORT_DEFS USE_BOOST_ASIO)
endif()

if(ARIABC_HAVE_KAFKA)
    add_compile_definitions(ARIABC_HAVE_RDKAFKA)
endif()

if(LIBPQ_INCLUDE_DIR AND LIBPQ_LIBRARY)
    add_executable(ariabc_pg_server
                   src/ariabc_pg_server.cxx
                   src/pg_state_machine.cxx
                   src/pg_executor.cxx
                   src/kafka_console.cxx
                   src/wire_protocol.cxx
                   ${ARIABC_COMMON_SRC})

    target_include_directories(ariabc_pg_server
                              PRIVATE
                                ${ARIABC_COMMON_INC}
                                ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include
                                ${LIBPQ_INCLUDE_DIR}
                                ${POSTGRES_INCLUDE_DIR}
                                ${RDKAFKA_INCLUDE_DIR})

    target_compile_definitions(ariabc_pg_server PRIVATE ${ARIABC_TRANSPORT_DEFS})

    target_link_libraries(ariabc_pg_server
                          NuRaft::static_lib
                          ${LIBPQ_LIBRARY}
                          ${ARIABC_PG_KAFKA_LIBS}
                          z
                          dl)
else()
    message(WARNING "libpq not found (libpq-fe.h / -lpq). Skipping ariabc_pg_server build.")
endif()

add_executable(ariabc_pg_gateway
               src/ariabc_pg_gateway.cxx
               src/async_cluster_submitter.cxx
               src/kafka_console.cxx
               src/wire_protocol_async.cxx
               src/wire_protocol.cxx)

target_include_directories(ariabc_pg_gateway
                          PRIVATE
                            ${ARIABC_COMMON_INC}
                            ${CMAKE_CURRENT_LIST_DIR}/../NuRaft/asio/asio/include
                            ${RDKAFKA_INCLUDE_DIR})

target_compile_definitions(ariabc_pg_gateway PRIVATE ${ARIABC_TRANSPORT_DEFS})

target_link_libraries(ariabc_pg_gateway
                      NuRaft::static_lib
                      ${ARIABC_PG_KAFKA_LIBS}
                      z
                      dl)
