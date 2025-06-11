# gtest unit test cases ###################################
add_executable(connections_test connections_test.cc)
target_link_libraries(connections_test ${TEST_LIBS})
gtest_discover_tests(connections_test)

add_executable(http_resource_resolver_test http_resource_resolver_test.cc)
target_link_libraries(http_resource_resolver_test ${TEST_LIBS})
gtest_discover_tests(http_resource_resolver_test)
