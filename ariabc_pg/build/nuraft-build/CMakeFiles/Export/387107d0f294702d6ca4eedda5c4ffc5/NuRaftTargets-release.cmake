#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "NuRaft::shared_lib" for configuration "Release"
set_property(TARGET NuRaft::shared_lib APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NuRaft::shared_lib PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnuraft.so"
  IMPORTED_SONAME_RELEASE "libnuraft.so"
  )

list(APPEND _cmake_import_check_targets NuRaft::shared_lib )
list(APPEND _cmake_import_check_files_for_NuRaft::shared_lib "${_IMPORT_PREFIX}/lib/libnuraft.so" )

# Import target "NuRaft::static_lib" for configuration "Release"
set_property(TARGET NuRaft::static_lib APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(NuRaft::static_lib PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libnuraft.a"
  )

list(APPEND _cmake_import_check_targets NuRaft::static_lib )
list(APPEND _cmake_import_check_files_for_NuRaft::static_lib "${_IMPORT_PREFIX}/lib/libnuraft.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
