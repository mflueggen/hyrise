function(add_plugin)
    cmake_parse_arguments(
        PARSED_ARGS
        ""
        "NAME"
        "SRCS;DEPS;LIBS"
        ${ARGN}
    )
    if(NOT PARSED_ARGS_NAME)
        message(FATAL_ERROR "You must provide a name for the plugin")
    endif(NOT PARSED_ARGS_NAME)

    add_library(${PARSED_ARGS_NAME} SHARED ${PARSED_ARGS_SRCS})

    add_dependencies(${PARSED_ARGS_NAME} hyrise)

    include_directories(${CMAKE_BINARY_DIR})  # For provider.hpp

    target_link_libraries(${PARSED_ARGS_NAME} ${PARSED_ARGS_LIBS})

    foreach(dep ${PARSED_ARGS_DEPS})
        add_dependencies(${PARSED_ARGS_NAME} ${dep})
    endforeach(dep)

    # Prevent the linker under macOS from complaining about undefined methods
    if (APPLE)
        if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
            target_link_libraries(${PARSED_ARGS_NAME} "-undefined dynamic_lookup")
        endif()
    endif()
endfunction(add_plugin)

add_plugin(NAME MvccDeletePlugin SRCS mvcc_delete_plugin.cpp mvcc_delete_plugin.hpp)
add_plugin(NAME hyriseTestPlugin SRCS test_plugin.cpp test_plugin.hpp)
add_plugin(NAME hyriseTestNonInstantiablePlugin SRCS non_instantiable_plugin.cpp)
# for pmdk?
include_directories(/usr/local/include)
# for pmdk
link_directories(/usr/local/lib)

set(UMAP_ROOT "~/umap/bin")
find_library(UMAP_LIB umap HINTS ${UMAP_ROOT}/lib)
add_plugin(NAME AntiCachingPlugin
    SRCS
    anti_caching/anti_caching_config.cpp
    anti_caching/anti_caching_config.hpp
    anti_caching/anti_caching_plugin.cpp
    anti_caching/anti_caching_plugin.hpp
    anti_caching/knapsack_solver.cpp
    anti_caching/knapsack_solver.hpp
    anti_caching/persistent_memory_manager.cpp
    anti_caching/persistent_memory_manager.hpp
    anti_caching/memory_resource/mmap_memory_resource.cpp
    anti_caching/memory_resource/mmap_memory_resource.hpp
#    anti_caching/memory_resource/pmemobj_memory_resource.cpp
#    anti_caching/memory_resource/pmemobj_memory_resource.hpp
    LIBS
    pmemobj
    ${UMAP_LIB})

target_include_directories(AntiCachingPlugin PRIVATE
    ${UMAP_ROOT}/include
    ${UMAP_ROOT}/include/umap
    ${UMAP_ROOT}/include/umap/store)


# We define TEST_PLUGIN_DIR to always load plugins from the correct directory for testing purposes
add_definitions(-DTEST_PLUGIN_DIR="${CMAKE_BINARY_DIR}/lib/")
