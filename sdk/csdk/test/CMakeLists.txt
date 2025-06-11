include (CTest)
include (GoogleTest)

set(GTEST_LIBS gtest gtest_main)
set(CSDK_LIBS tfoscsdk_shared)
set(TEST_LIBS ${GTEST_LIBS} ${CSDK_LIBS})

# gtest unit test cases ###################################
add_executable(modules_test modules_test.cc)
target_link_libraries(modules_test ${TEST_LIBS})
gtest_discover_tests(modules_test)

add_subdirectory("server-access")
add_subdirectory("tfos-common")
add_subdirectory("tfos-physical")
