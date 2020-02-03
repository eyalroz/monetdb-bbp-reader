
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        set(CXX_FILESYSTEM_LIBRARIES "stdc++fs")
endif()

add_executable(
	simple
	src/simple.cpp
)

add_executable(
	monetdb-bp-reader
	src/util/dump.cpp
	src/monetdb-bp-reader.cpp
)

foreach(t simple monetdb-bp-reader)
	target_include_directories(${t} PRIVATE "src/")
	target_link_libraries(${t} monetdb-buffer-pool ${CXX_FILESYSTEM_LIBRARIES})
	set_property(TARGET ${t} PROPERTY CXX_STANDARD 14)
	set_property(TARGET ${t} PROPERTY CXX_STANDARD_REQUIRED ON)
	set_property(TARGET ${t} PROPERTY CXX_EXTENSIONS OFF)
endforeach(t)

