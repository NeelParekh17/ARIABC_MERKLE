file(REMOVE_RECURSE
  "libnuraft.pdb"
  "libnuraft.so"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/shared_lib.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
