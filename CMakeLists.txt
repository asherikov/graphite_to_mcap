cmake_minimum_required(VERSION 3.8)
project(graphite_to_mcap VERSION 0.1.0 LANGUAGES CXX)

# -Wno-error=tsan
#   /usr/include/boost/asio/detail/std_fenced_block.hpp:46:29:
#   ‘atomic_thread_fence’ is not supported with ‘-fsanitize=thread’ [-Werror=tsan]
if(CCWS_CXX_FLAGS)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CCWS_CXX_FLAGS} -Wno-error=tsan")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${CCWS_LINKER_FLAGS}")
else()
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    set(CMAKE_VERBOSE_MAKEFILE ON)

    if(NOT CMAKE_CXX_STANDARD)
        set(CMAKE_CXX_STANDARD 17)
        set(CMAKE_CXX_STANDARD_REQUIRED ON)
    endif()

    if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        add_compile_options(-Wall -Wextra -Wpedantic -Werror -Wno-error=tsan)
    endif()
endif()

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN hidden)

find_package(Boost COMPONENTS headers REQUIRED)
find_package(pjmsg_mcap_wrapper REQUIRED)

if(CCWS_CLANG_TIDY)
    set(CMAKE_CXX_CLANG_TIDY "${CCWS_CLANG_TIDY}" CACHE STRING "" FORCE)
endif()

add_executable(${PROJECT_NAME}
    src/graphite_to_mcap.cpp
)

target_link_libraries(${PROJECT_NAME}
    PRIVATE "${Boost_LIBRARIES}"
    PRIVATE pjmsg_mcap_wrapper::pjmsg_mcap_wrapper
)

install(TARGETS ${PROJECT_NAME}
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)
