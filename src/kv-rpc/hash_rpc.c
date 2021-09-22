#include  "spdk/stdinc.h"

#include "hash.h"

#include "spdk/util.h"
#include "spdk/string.h"
#include "spdk/rpc.h"

#include "spdk/log.h"

struct rpc_construct_kv_hash {
	char *name;
	char *base_bdev;
	uint64_t meta_start_lba;
	uint64_t max_bucket_num;
};

static void
free_rpc_construct_kv_hash(struct rpc_construct_kv_hash *req)
{
	free(req->name);
	free(req->base_bdev);
}

static const struct spdk_json_object_decoder\
	rpc_construct_kv_hash_decoders[] = {
		{"name", offsetof(struct rpc_construct_kv_hash, name), 
			spdk_json_decode_string},
		{"base_bdev", offsetof(struct rpc_construct_kv_hash, base_bdev), 
			spdk_json_decode_string},
		{"meta_start_lba", offsetof(struct rpc_construct_kv_hash, meta_start_lba), 
			spdk_json_decode_uint64},
		{"max_bucket_num", offsetof(struct rpc_construct_kv_hash, max_bucket_num), 
			spdk_json_decode_uint64},
};

static void
rpc_kv_hash_create(struct spdk_jsonrpc_request *request,
		const struct spdk_json_val *params)
{
	struct rpc_construct_kv_hash req = {};
	struct spdk_json_write_ctx *w;
	int rc;

	if (spdk_json_decode_object(params, rpc_construct_kv_hash_decoders,
				SPDK_COUNTOF(rpc_construct_kv_hash_decoders), &req)) {
		SPDK_ERRLOG("Failed to decode kv hash create parameters.\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
				"Invalid parameters");
		goto create_cleanup;
	}

	rc = spdk_vbdev_kv_hash_create(req.base_bdev, req.name, 
			req.meta_start_lba, req.max_bucket_num);
	if (rc) {
		SPDK_ERRLOG("Failed to create kv hash vbdev: %s\n", spdk_strerror(-rc));
		spdk_jsonrpc_send_error_response_fmt(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
				"Failed to create kv hash vbdev: %s", spdk_strerror(-rc));
		goto create_cleanup;
	}

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_string(w, req.name);
	spdk_jsonrpc_end_result(request, w);

create_cleanup:
	free_rpc_construct_kv_hash(&req);
}
SPDK_RPC_REGISTER("bdev_kv_hash_create", rpc_kv_hash_create, SPDK_RPC_RUNTIME);

struct rpc_flush_kv_hash {
	char *name;
};

static void
free_rpc_flush_kv_hash(struct rpc_flush_kv_hash *req)
{
	free(req->name);
}

static const struct spdk_json_object_decoder\
	 rpc_flush_kv_hash_decoders[] = {
		 {"name", offsetof(struct rpc_flush_kv_hash, name), spdk_json_decode_string},
};

static void
_rpc_flush_kv_hash_cb(void *cb_ctx, int rc)
{
	struct spdk_jsonrpc_request *request = cb_ctx;
	struct spdk_json_write_ctx *w;

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_bool(w, rc == 0);
	spdk_jsonrpc_end_result(request, w);
}

static void
rpc_kv_hash_meta_flush(struct spdk_jsonrpc_request *request,
		const struct spdk_json_val *params)
{
	struct rpc_flush_kv_hash req = {};

	if (spdk_json_decode_object(params, rpc_flush_kv_hash_decoders,
				SPDK_COUNTOF(rpc_flush_kv_hash_decoders), &req)) {
		SPDK_ERRLOG("Failed to decode kv hash metadata flush parameters");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
				"Invalid parameters");
		goto del_cleanup;
	}

	spdk_vbdev_kv_hash_meta_flush(req.name, _rpc_flush_kv_hash_cb, request);

del_cleanup:
	free_rpc_flush_kv_hash(&req);
}
SPDK_RPC_REGISTER("bdev_kv_hash_meta_flush", rpc_kv_hash_meta_flush, SPDK_RPC_RUNTIME);

struct rpc_load_kv_hash {
	char *name;
	char *base_bdev;
	uint64_t meta_start_lba;
};

static void
free_rpc_load_kv_hash(struct rpc_load_kv_hash *req)
{
	free(req->name);
	free(req->base_bdev);
}

static const struct spdk_json_object_decoder
	rpc_load_kv_hash_decoders[] = {
		{"name", offsetof(struct rpc_load_kv_hash, name), 
			spdk_json_decode_string},
		{"base_bdev", offsetof(struct rpc_load_kv_hash, base_bdev),
			spdk_json_decode_string},
		{"meta_start_lba", offsetof(struct rpc_load_kv_hash, meta_start_lba),
			spdk_json_decode_uint64},
};

static void
_rpc_kv_hash_load_cb(void *cb_ctx, int rc)
{
	struct spdk_jsonrpc_request *request = cb_ctx;
	struct spdk_json_write_ctx *w;

	w = spdk_jsonrpc_begin_result(request);
	spdk_json_write_bool(w, rc == 0);
	spdk_jsonrpc_end_result(request, w);
}


static void
rpc_kv_hash_load(struct spdk_jsonrpc_request *request,
		const struct spdk_json_val *params)
{
	struct rpc_load_kv_hash req = {};
	int rc;
	if (spdk_json_decode_object(params, rpc_load_kv_hash_decoders,
				SPDK_COUNTOF(rpc_load_kv_hash_decoders), &req)) {
		SPDK_ERRLOG("Failed to decode kv hash load parameters.\n");
		spdk_jsonrpc_send_error_response(request, SPDK_JSONRPC_ERROR_INVALID_PARAMS,
				"Invalid parameters");
		goto load_cleanup;
	}

	rc = spdk_vbdev_kv_hash_load(req.name, req.base_bdev, req.meta_start_lba,
			_rpc_kv_hash_load_cb, request);
	
	if (rc)
	{
		SPDK_ERRLOG("Failed to load kv hash vbdev: %s\n", spdk_strerror(-rc));
		spdk_jsonrpc_send_error_response_fmt(request, SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
				"Failed to load kv hash vbdev: %s", spdk_strerror(-rc));
		goto load_cleanup;
	}

load_cleanup:
	free_rpc_load_kv_hash(&req);
}

SPDK_RPC_REGISTER("bdev_kv_hash_load", rpc_kv_hash_load, SPDK_RPC_RUNTIME);
