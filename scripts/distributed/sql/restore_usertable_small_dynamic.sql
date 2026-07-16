-- Compatibility entry point. Keep the 12k-row dataset in one canonical file.
\ir ../../restore_usertable_small.sql
\ir create_usertable_small_dynamic_index.sql
