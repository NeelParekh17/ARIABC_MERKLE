MODULE_big = merkle_lsm
OBJS = merkle_lsm.o merge.o $(WIN32RES)
EXTENSION = merkle_lsm
DATA = merkle_lsm--1.0.sql
PGFILEDESC = "merkle_lsm - multi-level Merkle lookup index"
REGRESS = merkle_lsm

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/merkle_lsm
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
