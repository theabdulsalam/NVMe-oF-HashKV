SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../../)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SO_VER := 1
SO_MINOR := 0
SO_SUFFIX := $(SO_VER).$(SO_MINOR)

CFLAGS += $(ENV_CFLAGS)

C_SRCS = hash.c hash_rpc.c
LIBNAME = bdev_kvhash

SPDK_MAP_FILE = $(SPDK_ROOT_DIR)/mk/spdk_blank.map
include $(SPDK_ROOT_DIR)/mk/spdk.lib.mk

