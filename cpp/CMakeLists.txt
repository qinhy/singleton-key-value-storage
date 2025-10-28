cmake_minimum_required(VERSION 3.10)
project(SingletonStorage)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-Duuid_system_generator)

# Include directories for header-only libraries
include_directories(include)

# Add source files
file(GLOB SOURCES "src/*.cpp")

# Create the executable
add_executable(SingletonStorage ${SOURCES})

find_package(ZLIB REQUIRED)
target_link_libraries(SingletonStorage PRIVATE ZLIB::ZLIB)

# nlohmann::json and stduuid are header-only, so no additional linking is needed here.
