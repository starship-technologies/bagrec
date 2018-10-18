project(bagrec)

find_package(catkin REQUIRED roscpp rosbag)

set(CMAKE_INCLUDE_CURRENT_DIR ON)

set(LIBCOMMON_INCLUDE_PATH deps/common_cxx)

catkin_package(INCLUDE_DIRS . ${LIBCOMMON_INCLUDE_PATH}
               CATKIN_DEPENDS rosbag topic_tools)

find_package(Boost REQUIRED COMPONENTS program_options)

include_directories(. ${catkin_INCLUDE_DIRS} ${LIBCOMMON_INCLUDE_PATH} ${Boost_INCLUDE_DIRS})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -W -Wno-maybe-uninitialized")

add_executable(bagrec bagrec.cpp)
target_link_libraries(bagrec ${catkin_LIBRARIES} ${Boost_LIBRARIES})

install(TARGETS bagrec
        RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

