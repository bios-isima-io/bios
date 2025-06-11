# gtest unit test cases ###################################
add_executable(event_loop_test event_loop_test.cc)
target_link_libraries(event_loop_test ${TEST_LIBS})
gtest_discover_tests(event_loop_test)

add_executable(message_handler_test message_handler_test.cc)
target_link_libraries(message_handler_test ${TEST_LIBS})
gtest_discover_tests(message_handler_test)

add_executable(object_mapper_test object_mapper_test.cc)
target_link_libraries(object_mapper_test ${TEST_LIBS})
gtest_discover_tests(object_mapper_test)

add_executable(statuses_test statuses_test.cc)
target_link_libraries(statuses_test ${TEST_LIBS})
gtest_discover_tests(statuses_test)

add_executable(utils_test utils_test.cc)
target_link_libraries(utils_test ${TEST_LIBS})
gtest_discover_tests(utils_test)
