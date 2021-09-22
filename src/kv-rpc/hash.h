#ifndef __KV_HASH_H_
#define __KV_HASH_H_

#include "spdk/bdev.h"
#include "spdk/bdev_module.h"

#define MAX_BUCKET_PER_PAGE 340
#define KV_HASH_SIG "KVS_HASH"

typedef void (*kv_hash_flush_cb)(void *ctx);
typedef void (*hash_load_completion_cb)(void *ctx, int rc);

int
spdk_vbdev_kv_hash_create(const char *bdev_name, const char *vbdev_name,
		uint64_t meta_start_lba, uint64_t max_bucket_num);

void
spdk_vbdev_kv_hash_meta_flush(const char *name, spdk_bdev_unregister_cb cb_fn,
		void *cb_arg);

int
spdk_vbdev_kv_hash_load(const char *vbdev_name, const char *bdev_name,
		uint64_t meta_start_lba, hash_load_completion_cb cb_fn, void *cb_arg);


#endif

