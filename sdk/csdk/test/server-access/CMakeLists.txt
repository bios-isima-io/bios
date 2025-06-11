# Tests in this directory require server access. The tests, thus, are not added to
# the CTest test list.

# gtest unit test cases ###################################
add_executable(dapi_server_test dapi_server_test.cc)
target_link_libraries(dapi_server_test ${TEST_LIBS})

add_executable(physical_pipe_impl_test physical_pipe_impl_test.cc)
target_link_libraries(physical_pipe_impl_test ${TEST_LIBS})

# no gtest test programs ##################################
add_executable(http2cli_test http2cli_test.cc)
target_link_libraries(http2cli_test ${CSDK_LIBS})
