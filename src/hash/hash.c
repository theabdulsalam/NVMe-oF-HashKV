#include "spdk/stdinc.h"

#include "hash.h"

#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/conf.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#include "spdk/nvme.h"

#include "spdk/log.h"

#define MAX_KEY_NUM 65535
#define BLOCK_SHIFT 12 // block size: 4K

static int kv_hash_init(void);
static int kv_hash_get_ctx_size(void);
static void kv_hash_finish(void);
static int kv_hash_config_json(struct spdk_json_write_ctx *w);
static void kv_hash_examine(struct spdk_bdev *bdev);

static struct spdk_bdev_module kv_hash_if = {
	.name = "kv_hash",
	.module_init = kv_hash_init,
	.module_fini = kv_hash_finish,
	.config_text = NULL,
	.config_json = kv_hash_config_json,
	.examine_config = kv_hash_examine,
	.get_ctx_size = kv_hash_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(kv_hash, &kv_hash_if);//remove when commit

struct kv_hash_config {
	char *vbdev_name;
	char *bdev_name;
	uint32_t meta_start_lba;
	uint32_t max_bucket_num;
	TAILQ_ENTRY(kv_hash_config) link;
};

struct bdev_kv_hash {
	struct spdk_bdev bdev;
	struct spdk_bdev_desc *base_desc;
	// fill necessary data fields
	struct hash_node *hash;
	uint32_t meta_start_lba;
	uint32_t max_bucket_num;
	struct spdk_io_channel *dev_channel;
	TAILQ_ENTRY(bdev_kv_hash) link;
	struct spdk_thread *thread;
};

static TAILQ_HEAD(, kv_hash_config) g_kv_hash_configs = TAILQ_HEAD_INITIALIZER(g_kv_hash_configs);

static TAILQ_HEAD(, bdev_kv_hash) g_bdev_hash_nodes = TAILQ_HEAD_INITIALIZER(g_bdev_hash_nodes);

struct kv_hash_io_channel {
	struct spdk_io_channel *base_ch;
};

// ???
struct kv_hash_io {
	struct bdev_kv_hash *bdev_hash;
};

struct hash_node {
	uint32_t key;
	uint32_t val_size; // val_size == val_size << 12
	uint32_t lba;
};

static int
kv_hash_init(void)
{
	return 0;
}

static void
kv_hash_remove_config(struct kv_hash_config *conf)
{
	TAILQ_REMOVE(&g_kv_hash_configs, conf, link);
	free(conf->vbdev_name);
	free(conf->bdev_name);
	free(conf);
}

static void
kv_hash_finish(void)
{
	struct kv_hash_config *conf;

	// TODO: update modified metadata on SSD
	while ((conf = TAILQ_FIRST(&g_kv_hash_configs))) {
		kv_hash_remove_config(conf);
	}
}

static int
kv_hash_config_json(struct spdk_json_write_ctx *w)
{
	struct bdev_kv_hash *bdev_node;
	struct spdk_bdev *base_bdev = NULL;

	TAILQ_FOREACH(bdev_node, &g_bdev_hash_nodes, link) {
		base_bdev = spdk_bdev_desc_get_bdev(bdev_node->base_desc);
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "bdev_kv_hash_create");
		spdk_json_write_named_object_begin(w, "params");
		spdk_json_write_named_string(w, "base_bdev", spdk_bdev_get_name(base_bdev));
		spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&bdev_node->bdev));
		spdk_json_write_named_uint64(w, "meta_start_lba", bdev_node->meta_start_lba);
		spdk_json_write_named_uint64(w, "max_bucket_num", bdev_node->max_bucket_num);
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);
	}
	return 0;
}

static int
kv_hash_get_ctx_size(void)
{
	return sizeof(struct kv_hash_io);
}

static void
_kv_hash_device_unregister_cb(void *io_device)
{
	struct bdev_kv_hash *bdev_node = io_device;

	SPDK_NOTICELOG("unregistering kv hash bdev.\n");
	free(bdev_node->bdev.name);
	free(bdev_node->hash);
	free(bdev_node);
	SPDK_NOTICELOG("kv hash bdev unregister finished.\n");
}

static void
_kv_hash_destruct(void *ctx)
{
	struct bdev_kv_hash *bdev_node = ctx;

	spdk_put_io_channel(bdev_node->dev_channel);
	spdk_bdev_close(bdev_node->base_desc);
}

static int
vbdev_kv_hash_destruct(void *ctx)
{
	struct bdev_kv_hash *bdev_node = (struct bdev_kv_hash *)ctx;

	TAILQ_REMOVE(&g_bdev_hash_nodes, bdev_node, link);

	SPDK_NOTICELOG("destruct kv-hash bdev.\n");
	spdk_bdev_module_release_bdev(spdk_bdev_desc_get_bdev(bdev_node->base_desc));
	if (bdev_node->thread && bdev_node->thread != spdk_get_thread()) {
		spdk_thread_send_msg(bdev_node->thread, _kv_hash_destruct, bdev_node);
	}
	else {
		spdk_put_io_channel(bdev_node->dev_channel);
		spdk_bdev_close(bdev_node->base_desc);
	}

	spdk_io_device_unregister(bdev_node, _kv_hash_device_unregister_cb);
	SPDK_NOTICELOG("vbdev kv hash destruct done.\n");

	return 0;
}

static void
_kv_hash_complete_get(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;

	/* Complete the original I/O and then free the one that we created here
	 * as a result of issuing an I/O via submit_request.
	 */

	spdk_bdev_io_complete(orig_io, status);
	spdk_bdev_free_io(bdev_io);
}

static void
kv_hash_get_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	struct bdev_kv_hash *kv_bdev = SPDK_CONTAINEROF(
			bdev_io->bdev, struct bdev_kv_hash, bdev);
	uint64_t key = bdev_io->u.bdev.offset_blocks;
	uint32_t val_size = bdev_io->u.bdev.num_blocks;
	struct kv_hash_io_channel *dev_ch = spdk_io_channel_get_ctx(ch);
	int rc;

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	if (kv_bdev->hash[key].val_size == 0) {
		SPDK_ERRLOG("[kv_hash] there is no data for key[%" PRIu64 "].\n", key);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	if (kv_bdev->hash[key].val_size < val_size) {
		SPDK_WARNLOG("[kv_hash] trying to get exceeding stored value size.\
[req: %" PRIu32 " > real: %" PRIu32 "]\n",
				val_size, kv_bdev->hash[key].val_size);
	}
	SPDK_DEBUGLOG(vbdev_kv_hash, "Requesting key %" PRIu64 " (lba: %d)\n",
			key, kv_bdev->hash[key].lba);

	if(bdev_io->u.bdev.md_buf == NULL) {
		rc = spdk_bdev_readv_blocks(kv_bdev->base_desc, dev_ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				kv_bdev->hash[key].lba, val_size,
				_kv_hash_complete_get, bdev_io);
	}
	else {
		rc = spdk_bdev_readv_blocks_with_md(kv_bdev->base_desc, dev_ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				bdev_io->u.bdev.md_buf,
				kv_bdev->hash[key].lba, val_size,
				_kv_hash_complete_get, bdev_io);
	}

	if (rc) {
		SPDK_ERRLOG("ERROR on bdev_io submission: %d\n", rc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static int
kv_hash_get(struct bdev_kv_hash *bdev_node, struct kv_hash_io_channel *ch,
		struct spdk_bdev_io *bdev_io)
{
	uint64_t key = bdev_io->u.bdev.offset_blocks;
	uint32_t val_size = bdev_io->u.bdev.num_blocks;
	int rc;

	// TODO: some other err checking or ..?
	if (bdev_node->hash[key].val_size == 0) {
		SPDK_ERRLOG("[kv_hash] there is no data for key[%" PRIu64 "].\n", key);
		return -EINVAL;
	}

	if (bdev_node->hash[key].val_size < val_size) {
		SPDK_WARNLOG("[kv_hash] trying to get exceeding stored value size.\
[req: %" PRIu32 " > real: %" PRIu32 "]\n",
				val_size, bdev_node->hash[key].val_size);
	}
	SPDK_DEBUGLOG(vbdev_kv_hash, "Requesting key %" PRIu64 " (lba: %d)\n",
			key, bdev_node->hash[key].lba);

	if(bdev_io->u.bdev.md_buf == NULL) {
		rc = spdk_bdev_readv_blocks(bdev_node->base_desc, ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				bdev_node->hash[key].lba, val_size,
				_kv_hash_complete_get, bdev_io);
	}
	else {
		rc = spdk_bdev_readv_blocks_with_md(bdev_node->base_desc, ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				bdev_io->u.bdev.md_buf,
				bdev_node->hash[key].lba, val_size,
				_kv_hash_complete_get, bdev_io);
	}

	return rc;
}

static void
_kv_hash_complete_put(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;

	/* Update hash node */
	if(status == SPDK_BDEV_IO_STATUS_SUCCESS)
	{
		struct bdev_kv_hash *bdev_node = SPDK_CONTAINEROF(orig_io->bdev,
				struct bdev_kv_hash, bdev);
		struct hash_node new_node;
		new_node.key = orig_io->u.bdev.offset_blocks;
		new_node.lba = bdev_io->u.bdev.offset_blocks;
		new_node.val_size = bdev_io->u.bdev.num_blocks;
		bdev_node->hash[new_node.key] = new_node;
		SPDK_DEBUGLOG(vbdev_kv_hash, "key: %d lba: %d val_size: %d\n",
				new_node.key, new_node.lba, new_node.val_size);
	}

	/* Complete the original I/O and then free the one that we created here
	 * as a result of issuing an I/O via submit_request.
	 */
	// IO passing inside bdev request --> new bdev_io generated
	// vbdev always redirect request to base bdev --> new bdev io for base bdev needed
	spdk_bdev_io_complete(orig_io, status);
	spdk_bdev_free_io(bdev_io);
}

static int
kv_hash_put(struct bdev_kv_hash *bdev_node, struct kv_hash_io_channel *ch,
		struct spdk_bdev_io *bdev_io)
{
	uint64_t key = bdev_io->u.bdev.offset_blocks;
	uint32_t val_size = bdev_io->u.bdev.num_blocks;
	int rc = 0;

	// TODO: lba selection for key? current: key*100
	if(bdev_io->u.bdev.md_buf == NULL) {
		rc = spdk_bdev_writev_blocks(bdev_node->base_desc, ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				200+key*512, val_size,
				_kv_hash_complete_put, bdev_io);
	}
	else {
		rc = spdk_bdev_writev_blocks_with_md(bdev_node->base_desc, ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				bdev_io->u.bdev.md_buf,
				200+key*512, val_size,
				_kv_hash_complete_put, bdev_io);
	}


	return rc;
}

static void
_kv_hash_router_complete_read_write(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct spdk_bdev_io *orig_io = cb_arg;
	int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;

	spdk_bdev_io_complete(orig_io, status);
	spdk_bdev_free_io(bdev_io);
}

static int
kv_hash_router_read(struct bdev_kv_hash *bdev_node, struct kv_hash_io_channel *ch,
		struct spdk_bdev_io *bdev_io)
{
	int rc;
	if (bdev_io->u.bdev.md_buf == NULL){
		rc = spdk_bdev_readv_blocks(bdev_node->base_desc, ch->base_ch, bdev_io->u.bdev.iovs,
				bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.num_blocks,
				bdev_io->u.bdev.offset_blocks, _kv_hash_router_complete_read_write,
				bdev_io);
	}
	else {
		rc = spdk_bdev_readv_blocks_with_md(bdev_node->base_desc, ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, 
				bdev_io->u.bdev.md_buf,
				bdev_io->u.bdev.num_blocks,	bdev_io->u.bdev.offset_blocks, 
				_kv_hash_router_complete_read_write, bdev_io);

	}

	return rc;
}

static void
kv_hash_router_read_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
	int rc;
	struct bdev_kv_hash *bdev_node = SPDK_CONTAINEROF(bdev_io->bdev,
			struct bdev_kv_hash, bdev);
	struct kv_hash_io_channel *dev_ch = spdk_io_channel_get_ctx(ch);

	if (!success) {
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	if (bdev_io->u.bdev.md_buf == NULL) {
		rc = spdk_bdev_readv_blocks(bdev_node->base_desc, dev_ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
				bdev_io->u.bdev.offset_blocks, bdev_io->u.bdev.num_blocks,
				_kv_hash_router_complete_read_write, bdev_io);
	}
	else {
		rc = spdk_bdev_readv_blocks_with_md(bdev_node->base_desc, dev_ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, 
				bdev_io->u.bdev.md_buf,
				bdev_io->u.bdev.num_blocks,	bdev_io->u.bdev.offset_blocks, 
				_kv_hash_router_complete_read_write, bdev_io);
	}

	if (rc) {
		SPDK_ERRLOG("ERROR on bdev_io submission: %d\n", rc);
		spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static int
kv_hash_router_write(struct bdev_kv_hash *bdev_node, struct kv_hash_io_channel *ch,
		struct spdk_bdev_io *bdev_io)
{
	int rc;
	if (bdev_io->u.bdev.md_buf == NULL){
		rc = spdk_bdev_writev_blocks(bdev_node->base_desc, ch->base_ch, bdev_io->u.bdev.iovs,
				bdev_io->u.bdev.iovcnt, bdev_io->u.bdev.num_blocks,
				bdev_io->u.bdev.offset_blocks, _kv_hash_router_complete_read_write,
				bdev_io);
	}
	else {
		rc = spdk_bdev_writev_blocks_with_md(bdev_node->base_desc, ch->base_ch,
				bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt, 
				bdev_io->u.bdev.md_buf,
				bdev_io->u.bdev.num_blocks,	bdev_io->u.bdev.offset_blocks, 
				_kv_hash_router_complete_read_write, bdev_io);

	}

	return rc;

}

static void
vbdev_kv_hash_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
	struct bdev_kv_hash *bdev_node = SPDK_CONTAINEROF(bdev_io->bdev,
			struct bdev_kv_hash, bdev);
	struct kv_hash_io_channel *dev_ch = spdk_io_channel_get_ctx(ch);
	int rc = 0;

	SPDK_NOTICELOG("kv hash submitting request. type: %d\n", bdev_io->type);
	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_KV_GET:
		rc = kv_hash_get(bdev_node, dev_ch, bdev_io);
		//spdk_bdev_io_get_buf(bdev_io, kv_hash_get_cb,
		//		bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		break;
	case SPDK_BDEV_IO_TYPE_KV_PUT:
		rc = kv_hash_put(bdev_node, dev_ch, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_READ:
		spdk_bdev_io_get_buf(bdev_io, kv_hash_router_read_cb,
				bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
		//rc = kv_hash_router_read(bdev_node, dev_ch, bdev_io);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		rc = kv_hash_router_write(bdev_node, dev_ch, bdev_io);
		break;
	default:
		SPDK_ERRLOG("vbdev_block[kv_hash]: unknown I/O type %u\n", bdev_io->type);
		rc = -ENOTSUP;
		break;
	}

	if (rc != 0) {
		if (rc == -ENOMEM) {
			SPDK_WARNLOG("ENOMEM, start to queue io for vbdev[kv_hash].\n");
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
		}
		else {
			SPDK_ERRLOG("ERROR on bdev_io submission! (err: %d)\n", rc);
			spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	}
}

static bool
vbdev_kv_hash_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
	switch (io_type) {
	case SPDK_BDEV_IO_TYPE_KV_GET:
	case SPDK_BDEV_IO_TYPE_KV_PUT:
		return true;
	default:
		return false;
	}
}

static struct spdk_io_channel *
vbdev_kv_hash_get_io_channel(void *ctx)
{
	struct bdev_kv_hash *bdev_node = (struct bdev_kv_hash *)ctx;

	return spdk_get_io_channel(bdev_node);
}

static int
vbdev_kv_hash_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct bdev_kv_hash *bdev_node = (struct bdev_kv_hash *)ctx;

	spdk_json_write_name(w, "kv_hash");
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", spdk_bdev_get_name(&bdev_node->bdev));
	spdk_json_write_named_string(w, "base_bdev", spdk_bdev_get_name(
				spdk_bdev_desc_get_bdev(bdev_node->base_desc)));
	spdk_json_write_named_uint64(w, "meta_start_lba", bdev_node->meta_start_lba);
	spdk_json_write_named_uint64(w, "max_bucket_num", bdev_node->max_bucket_num);
	spdk_json_write_object_end(w);

	return 0;
}

static const struct spdk_bdev_fn_table kv_hash_fn_table = {
	.destruct = vbdev_kv_hash_destruct,
	.submit_request = vbdev_kv_hash_submit_request,
	.io_type_supported = vbdev_kv_hash_io_type_supported,
	.get_io_channel = vbdev_kv_hash_get_io_channel,
	.dump_info_json = vbdev_kv_hash_dump_info_json,
	//.write_config_json = vbdev_kv_hash_write_config_json,
};

static struct kv_hash_config *
kv_hash_get_conf(const char *bdev_name, const char *vbdev_name)
{
	struct kv_hash_config *conf;
	TAILQ_FOREACH(conf, &g_kv_hash_configs, link) {
		if (strcmp(vbdev_name, conf->vbdev_name) == 0 &&
			strcmp(bdev_name, conf->bdev_name) == 0) {
			return conf;
		}
	}
	return NULL;
}

static int
kv_hash_insert_conf(const char *bdev_name, const char *vbdev_name,
			uint64_t meta_start_lba, uint64_t max_bucket_num)
{
	struct kv_hash_config *conf;

	conf = kv_hash_get_conf(bdev_name, vbdev_name);
	if (conf) {
		if(strcmp(vbdev_name, conf->vbdev_name) == 0) {
			SPDK_ERRLOG("kv hash bdev %s already exists.\n", vbdev_name);
		}
		if (strcmp(bdev_name, conf->bdev_name) == 0) {
			SPDK_ERRLOG("base bdev %s already claimed.\n", bdev_name);
		}
		return -EEXIST;
	}

	conf = calloc(1, sizeof(*conf));
	if (!conf) {
		SPDK_ERRLOG("could not allocate kv hash config node.\n");
		return -ENOMEM;
	}

	conf->bdev_name = strdup(bdev_name);
	if (!conf->bdev_name) {
		SPDK_ERRLOG("could not allocate conf->bdev_name.\n");
		free(conf);
		return -ENOMEM;
	}
	conf->vbdev_name = strdup(vbdev_name);
	if (!conf->vbdev_name) {
		SPDK_ERRLOG("could not allocate conf->vbdev_name.\n");
		free(conf->bdev_name);
		free(conf);
		return -ENOMEM;
	}

	conf->meta_start_lba = meta_start_lba;
	conf->max_bucket_num = max_bucket_num;

	TAILQ_INSERT_TAIL(&g_kv_hash_configs, conf, link);

	return 0;
}

static int
kv_hash_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct kv_hash_io_channel *bdev_ch = ctx_buf;
	struct bdev_kv_hash *bdev_node = io_device;

	bdev_ch->base_ch = spdk_bdev_get_io_channel(bdev_node->base_desc);
	if (!bdev_ch->base_ch) {
		return -ENOMEM;
	}

	return 0;
}

static void
kv_hash_ch_destroy_cb(void *io_device, void *ctx_buf)
{
	struct kv_hash_io_channel *bdev_ch = ctx_buf;

	SPDK_NOTICELOG("destroying kv hash io channel.\n");
	spdk_put_io_channel(bdev_ch->base_ch);
	SPDK_NOTICELOG("destroyed well kv hash io channel.\n");
}

static void
kv_hash_base_bdev_hotremove_cb(void *ctx)
{
	struct bdev_kv_hash *bdev_node, *tmp;
	struct spdk_bdev *bdev_find = ctx;

	TAILQ_FOREACH_SAFE(bdev_node, &g_bdev_hash_nodes, link, tmp) {
		if (bdev_find == spdk_bdev_desc_get_bdev(bdev_node->base_desc)) {
			spdk_bdev_unregister(&bdev_node->bdev, NULL, NULL);
		}
	}
}

static void
vbdev_kv_hash_base_bdev_event_cb(enum spdk_bdev_event_type type,
		struct spdk_bdev *bdev, void *event_ctx)
{
	// bdev == base_bdev
	switch (type) {
		case SPDK_BDEV_EVENT_REMOVE:
			// TODO: Check this. Additional action needed?
			spdk_vbdev_kv_hash_meta_flush(bdev->name, NULL, NULL);
			break;
		default:
			SPDK_WARNLOG("Unsupported bdev event: type %d\n", type);
			break;
	}
}

static int
kv_hash_register_remain(struct spdk_bdev *base_bdev, struct bdev_kv_hash *kv_bdev)
{
	int rc = 0;
	struct kv_hash_config *conf;

	conf = kv_hash_get_conf(base_bdev->name, kv_bdev->bdev.name);

	SPDK_NOTICELOG("vbdev name: %s\n", kv_bdev->bdev.name);
	kv_bdev->meta_start_lba = conf->meta_start_lba;
	kv_bdev->max_bucket_num = conf->max_bucket_num;

	kv_bdev->bdev.product_name = "kv_hash";

	kv_bdev->bdev.write_cache = base_bdev->write_cache;
	kv_bdev->bdev.required_alignment = base_bdev->required_alignment;
	kv_bdev->bdev.optimal_io_boundary = base_bdev->optimal_io_boundary;
	kv_bdev->bdev.blocklen = base_bdev->blocklen;
	kv_bdev->bdev.blockcnt = base_bdev->blockcnt;

		// setups for metadata
	kv_bdev->bdev.md_interleave = base_bdev->md_interleave;
	kv_bdev->bdev.md_len = base_bdev->md_len;
	kv_bdev->bdev.dif_type = base_bdev->dif_type;
	kv_bdev->bdev.dif_is_head_of_md = base_bdev->dif_is_head_of_md;
	kv_bdev->bdev.dif_check_flags = base_bdev->dif_check_flags;

	/* This is the context that is passed to us when the bdev
	 * layer calls in so we'll save our bdev node here.
	 */
	kv_bdev->bdev.ctxt = kv_bdev;
	kv_bdev->bdev.fn_table = &kv_hash_fn_table;
	kv_bdev->bdev.module = &kv_hash_if;

	// TODO: bdev specific info needed?

	TAILQ_INSERT_TAIL(&g_bdev_hash_nodes, kv_bdev, link);

	spdk_io_device_register(kv_bdev,
			kv_hash_ch_create_cb, kv_hash_ch_destroy_cb,
			sizeof(struct kv_hash_io_channel), conf->vbdev_name);

	// TODO: spdk_bdev_open is deprecated. Use open_ext, instead.
	rc = spdk_bdev_open_ext(base_bdev->name, true, vbdev_kv_hash_base_bdev_event_cb,
			NULL, &kv_bdev->base_desc);
	//rc = spdk_bdev_open(base_bdev, true, kv_hash_base_bdev_hotremove_cb,
	//		base_bdev, &kv_bdev->base_desc);

	if (rc) {
		SPDK_ERRLOG("could not open base bdev %s.\n",
				spdk_bdev_get_name(base_bdev));
		goto remain_base_bdev_open_failed;
	}

	kv_bdev->thread = spdk_get_thread();

	rc = spdk_bdev_module_claim_bdev(base_bdev, kv_bdev->base_desc,
			kv_bdev->bdev.module);
	if (rc) {
		SPDK_ERRLOG("could not claim base bdev %s.\n",
				spdk_bdev_get_name(base_bdev));
		goto remain_base_bdev_claim_failed;
	}

	// KV hash init on SSD is not needed -- create/update when close
	//spdk_kv_hash_init(kv_bdev);

	rc = spdk_bdev_register(&kv_bdev->bdev);
	if (rc) {
		SPDK_ERRLOG("could not register kv-hash bdev.\n");
		goto remain_register_failed;
	}

	// self contain io_channel for metadata write
	//kv_bdev->dev_channel = spdk_get_io_channel(kv_bdev);
	kv_bdev->dev_channel = spdk_bdev_get_io_channel(kv_bdev->base_desc);
	SPDK_NOTICELOG("kv-hash bdev successfully generated!\n");

	return rc;

remain_register_failed:
	spdk_bdev_module_release_bdev(&kv_bdev->bdev);
remain_base_bdev_claim_failed:
	spdk_bdev_close(kv_bdev->base_desc);
remain_base_bdev_open_failed:
	TAILQ_REMOVE(&g_bdev_hash_nodes, kv_bdev, link);
	spdk_io_device_unregister(kv_bdev, NULL);
	free(kv_bdev->hash);
	free(kv_bdev->bdev.name);
	free(kv_bdev);
	kv_hash_remove_config(conf);
	return rc;
}

static int
kv_hash_register(struct spdk_bdev *base_bdev)
{
	struct kv_hash_config *conf, *tmp;
	struct bdev_kv_hash *bdev_node;
	int rc = 0;

	/* Check our list of names from config versus this bdev and if
	 * there's a match, create the bdev_node & bdev accordingly.
	 */
	TAILQ_FOREACH_SAFE(conf, &g_kv_hash_configs, link, tmp) {
		if (strcmp(conf->bdev_name, base_bdev->name) != 0) {
			continue;
		}

		// TODO: kv hash bdev check?

		bdev_node = calloc(1, sizeof(struct bdev_kv_hash));
		if (!bdev_node) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate bdev_node.\n");
			goto free_config;
		}

		bdev_node->bdev.name = strdup(conf->vbdev_name);
		if (!bdev_node->bdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate string for bdev name.\n");
			goto strdup_failed;
		}

		bdev_node->meta_start_lba = conf->meta_start_lba;
		bdev_node->max_bucket_num = conf->max_bucket_num;
		bdev_node->hash = calloc(conf->max_bucket_num, sizeof(struct hash_node));
		if (!bdev_node->hash) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate hash.\n");
			goto hash_alloc_failed;
		}

		bdev_node->bdev.product_name = "kv_hash";

		/* Copy some properties from the underlying base bdev. */
		bdev_node->bdev.write_cache = base_bdev->write_cache;
		bdev_node->bdev.required_alignment = base_bdev->required_alignment;
		bdev_node->bdev.optimal_io_boundary = base_bdev->optimal_io_boundary;
		bdev_node->bdev.blocklen = base_bdev->blocklen;
		bdev_node->bdev.blockcnt = base_bdev->blockcnt;

		// setups for metadata
		bdev_node->bdev.md_interleave = base_bdev->md_interleave;
		bdev_node->bdev.md_len = base_bdev->md_len;
		bdev_node->bdev.dif_type = base_bdev->dif_type;
		bdev_node->bdev.dif_is_head_of_md = base_bdev->dif_is_head_of_md;
		bdev_node->bdev.dif_check_flags = base_bdev->dif_check_flags;

		/* This is the context that is passed to us when the bdev
		 * layer calls in so we'll save our bdev node here.
		 */
		bdev_node->bdev.ctxt = bdev_node;
		bdev_node->bdev.fn_table = &kv_hash_fn_table;
		bdev_node->bdev.module = &kv_hash_if;

		// TODO: bdev specific info needed?

		TAILQ_INSERT_TAIL(&g_bdev_hash_nodes, bdev_node, link);

		spdk_io_device_register(bdev_node,
				kv_hash_ch_create_cb, kv_hash_ch_destroy_cb,
				sizeof(struct kv_hash_io_channel), conf->vbdev_name);

		// TODO: spdk_bdev_open is deprecated. Use open_ext, instead.
		rc = spdk_bdev_open_ext(base_bdev->name, true, vbdev_kv_hash_base_bdev_event_cb,
				NULL, &bdev_node->base_desc);
		//rc = spdk_bdev_open(base_bdev, true, kv_hash_base_bdev_hotremove_cb,
		//		base_bdev, &bdev_node->base_desc);

		if (rc) {
			SPDK_ERRLOG("could not open base bdev %s.\n",
					spdk_bdev_get_name(base_bdev));
			goto base_bdev_open_failed;
		}

		bdev_node->thread = spdk_get_thread();

		rc = spdk_bdev_module_claim_bdev(base_bdev, bdev_node->base_desc,
				bdev_node->bdev.module);
		if (rc) {
			SPDK_ERRLOG("could not claim base bdev %s.\n",
					spdk_bdev_get_name(base_bdev));
			goto base_bdev_claim_failed;
		}

		// KV hash init on SSD is not needed -- create/update when close
		//spdk_kv_hash_init(bdev_node);

		rc = spdk_bdev_register(&bdev_node->bdev);
		if (rc) {
			SPDK_ERRLOG("could not register kv-hash bdev.\n");
			goto register_failed;
		}
		
		// self contain io_channel for metadata write
		//bdev_node->dev_channel = spdk_get_io_channel(bdev_node);
		bdev_node->dev_channel = spdk_bdev_get_io_channel(bdev_node->base_desc);
		SPDK_NOTICELOG("kv-hash bdev successfully generated!\n");
	}
	return rc;

register_failed:
	spdk_bdev_module_release_bdev(&bdev_node->bdev);
base_bdev_claim_failed:
	spdk_bdev_close(bdev_node->base_desc);
base_bdev_open_failed:
	TAILQ_REMOVE(&g_bdev_hash_nodes, bdev_node, link);
	spdk_io_device_unregister(bdev_node, NULL);
	free(bdev_node->hash);
hash_alloc_failed:
	free(bdev_node->bdev.name);
strdup_failed:
	free(bdev_node);
free_config:
	kv_hash_remove_config(conf);
	return rc;
}

int
spdk_vbdev_kv_hash_create(const char *bdev_name, const char *vbdev_name,
		uint64_t meta_start_lba, uint64_t max_bucket_num)
{
	struct spdk_bdev *bdev = NULL;
	int rc;

	if(max_bucket_num == 0)
		max_bucket_num = MAX_KEY_NUM + 1;

	rc = kv_hash_insert_conf(bdev_name, vbdev_name, meta_start_lba, max_bucket_num);
	if (rc) {
		return rc;
	}

	bdev = spdk_bdev_get_by_name(bdev_name);
	if (!bdev) {
		/* This is not an error, even though the bdev is not present at this time it may
		 * still show up later.
		 */
		return 0;
	}

	return kv_hash_register(bdev);
}

struct kv_hash_meta_flush_ctx {
	kv_hash_flush_cb cb;
	void *cb_args;
	void *payload;
};

/* sizeof(struct kv_hash_meta) == 4096 */
struct kv_hash_meta {
	char signature[8];
	uint32_t buck_size;
	struct hash_node info[MAX_BUCKET_PER_PAGE];
	uint32_t next_lba;
};

static void
_kv_hash_complete_meta_flush(struct spdk_bdev_io *bdev_io, bool success,
		void *cb_arg)
{
	struct kv_hash_meta_flush_ctx *flush_ctx = cb_arg;
	//int status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;

	/* cb: _kv_hash_flush_on_destruct */
	fprintf(stderr, "payload addr: %p\n", flush_ctx->payload);
	spdk_free(flush_ctx->payload);
	if (flush_ctx->cb)
		flush_ctx->cb(flush_ctx->cb_args);
	free(flush_ctx);

	spdk_bdev_free_io(bdev_io);
}

static int
kv_hash_construct_meta(struct bdev_kv_hash *kv_bdev, struct kv_hash_meta **_meta_info)
{
	struct kv_hash_meta *meta_info;
	uint32_t blocklen = kv_bdev->bdev.blocklen;
	unsigned short skip_addr = PAGE_SIZE / blocklen;
	uint32_t next_lba = 0;
	uint32_t max_bucket_num = kv_bdev->max_bucket_num;
	unsigned short num_meta_page = (max_bucket_num % MAX_BUCKET_PER_PAGE == 0)
		? (max_bucket_num / MAX_BUCKET_PER_PAGE)
		: (max_bucket_num / MAX_BUCKET_PER_PAGE + 1);

	meta_info = spdk_zmalloc(sizeof(*meta_info) * num_meta_page, 0x1000, NULL,
			SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	if (!meta_info) {
		return -ENOMEM;
	}

	int j = 0;
	memcpy(meta_info[j].signature, KV_HASH_SIG,
			sizeof(meta_info[j].signature));
	meta_info[j].buck_size = max_bucket_num;

	for(uint32_t i = 0; i < max_bucket_num; ++i){
		meta_info[j].info[i%MAX_BUCKET_PER_PAGE] = kv_bdev->hash[i];
		if((i + 1 < (max_bucket_num-1)) && ((i + 1) % MAX_BUCKET_PER_PAGE == 0)){
			next_lba += skip_addr;
			meta_info[j].next_lba = next_lba;
			++j;
			memcpy(meta_info[j].signature, KV_HASH_SIG,
					sizeof(meta_info[j].signature));
		}
	}

	*_meta_info = meta_info;
	return num_meta_page;
}

static void
kv_hash_meta_flush(struct spdk_bdev *bdev, kv_hash_flush_cb flush_cb, void *args)
{
	struct bdev_kv_hash *kv_bdev = SPDK_CONTAINEROF(bdev, struct bdev_kv_hash, bdev);
	struct kv_hash_meta *meta_info;
	struct kv_hash_meta_flush_ctx *flush_ctx;
	uint32_t start_lba = kv_bdev->meta_start_lba;
	int num_pages;


	num_pages = kv_hash_construct_meta(kv_bdev, &meta_info);
	if (num_pages < 0){
		return;
	}
	SPDK_NOTICELOG("Flushing metadata to lba:%d(page: %d).\n", start_lba, num_pages);
	
	flush_ctx = calloc(1, sizeof(*flush_ctx));
	if (!flush_ctx) {
		spdk_free(meta_info);
		return;
	}

	flush_ctx->payload = meta_info;
	flush_ctx->cb = flush_cb;
	flush_ctx->cb_args = args;

	// spdk_bdev_write_blocks(desc, channel, payload, lba,
	// 		lba_count,complete_cb, cb_args);
	spdk_bdev_write_blocks(kv_bdev->base_desc, kv_bdev->dev_channel,
			meta_info, start_lba,
			(sizeof(*meta_info)*num_pages) / kv_bdev->bdev.blocklen,
			_kv_hash_complete_meta_flush, flush_ctx);

}

struct kv_hash_delete_ctx {
	struct spdk_bdev *bdev;
	spdk_bdev_unregister_cb cb_fn;
	void *cb_arg;
};

static void
_kv_hash_meta_flush(void *ctx)
{
	struct kv_hash_delete_ctx *del_ctx = ctx;
	del_ctx->cb_fn(del_ctx->cb_arg, 0);
	SPDK_NOTICELOG("KV-hash bdev deleted well.\n");
	free(del_ctx);
}

void
spdk_vbdev_kv_hash_meta_flush(const char *name, spdk_bdev_unregister_cb cb_fn,
		void *cb_arg)
{
	//struct kv_hash_config *conf;
	struct spdk_bdev *bdev = NULL;
	struct kv_hash_delete_ctx *ctx;

	bdev = spdk_bdev_get_by_name(name);
	if (!bdev || bdev->module != &kv_hash_if) {
		if (cb_fn)
			cb_fn(cb_arg, -ENODEV);
		return;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx) {
		if (cb_fn)
			cb_fn(cb_arg, -ENOMEM);
		return;
	}
	ctx->bdev = bdev;
	ctx->cb_fn = cb_fn;
	ctx->cb_arg = cb_arg;
	/*
	TAILQ_FOREACH(conf, &g_kv_hash_configs, link) {
		if (!strcmp(conf->vbdev_name, bdev->name)) {
			kv_hash_remove_config(conf);
			break;
		}
	}
	*/
	/* unregister calls destruct function inside */
	/*
	spdk_bdev_unregister(bdev, cb_fn, cb_arg);
	*/
	kv_hash_meta_flush(bdev, _kv_hash_meta_flush, ctx);
}

struct kv_hash_load_ctx {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *desc;
	struct spdk_io_channel *ch;
	struct bdev_kv_hash *kv_bdev;
	char *vbdev_name;

	void *payload;
	int meta_num;
	uint32_t loaded_bucket;
	uint32_t meta_start_lba;

	hash_load_completion_cb cb;
	void *cb_arg;
};

static void
_kv_hash_complete_load_more(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct kv_hash_load_ctx *ctx = cb_arg;
	struct kv_hash_meta *meta_info = ctx->payload;
	int loaded_page = ctx->meta_num;
	int bucket_idx = ctx->loaded_bucket;
	struct bdev_kv_hash *kv_bdev = ctx->kv_bdev;
	uint32_t tot_bucket_size = kv_bdev->max_bucket_num;
	uint32_t remain_bucket = tot_bucket_size - MAX_BUCKET_PER_PAGE;
	int rc = 0;


	SPDK_DEBUGLOG(vbdev_kv_hash, "[key:%d] val_size: %d lba: %d\n",
			1, kv_bdev->hash[1].val_size, kv_bdev->hash[1].lba);

	for(int i = 0; i < loaded_page; ++i){
		uint32_t loadable_bucket = (remain_bucket > MAX_BUCKET_PER_PAGE) ?
				MAX_BUCKET_PER_PAGE : remain_bucket;
		if(memcmp(meta_info[i].signature, KV_HASH_SIG,
					sizeof(meta_info[i].signature)) != 0 ) {
			SPDK_WARNLOG("metadata page spoiled.\n");
			break;
		}
		SPDK_DEBUGLOG(vbdev_kv_hash, "metadata next lba: %d\n",
				meta_info[i].next_lba);

		for(uint32_t j = 0; j < loadable_bucket; ++j) {
			kv_bdev->hash[bucket_idx++] = meta_info[i].info[j];
		}
		SPDK_DEBUGLOG(vbdev_kv_hash, "[key:%d] val_size: %d lba: %d\n",
				bucket_idx - loadable_bucket + 1,
				kv_bdev->hash[bucket_idx - loadable_bucket + 1].val_size,
				kv_bdev->hash[bucket_idx - loadable_bucket + 1].lba);

		remain_bucket -= loadable_bucket;
	}

	SPDK_NOTICELOG("%d buckets loaded.\n", tot_bucket_size-remain_bucket);

	spdk_put_io_channel(ctx->ch);
	spdk_bdev_close(ctx->desc);
	spdk_free(meta_info);

	rc = kv_hash_register_remain(ctx->bdev, kv_bdev);
	ctx->cb(ctx->cb_arg, rc);

	free(ctx);
	spdk_bdev_free_io(bdev_io);
}

static int
kv_hash_load_more(int meta_start_lba, struct kv_hash_load_ctx *ctx)
{
	struct kv_hash_meta *meta_info = ctx->payload;
	struct kv_hash_meta *new_meta;
	int bucket_remain = meta_info->buck_size - ctx->loaded_bucket;
	int extra_page = bucket_remain / MAX_BUCKET_PER_PAGE;

	if (bucket_remain % MAX_BUCKET_PER_PAGE) extra_page++;

	new_meta = spdk_zmalloc(sizeof(*new_meta) * extra_page, 0x1000, NULL,
			SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	if (!new_meta) {
		SPDK_ERRLOG("could not allocate meta info with spdk_zmalloc.\n");
		return -ENOMEM;
	}
	spdk_free(meta_info);

	ctx->payload = new_meta;
	ctx->meta_num = extra_page;
	ctx->meta_start_lba = meta_start_lba;

	return spdk_bdev_read_blocks(ctx->desc, ctx->ch, new_meta, meta_start_lba,
			(sizeof(*new_meta)*extra_page/ctx->bdev->blocklen),
			_kv_hash_complete_load_more, ctx);
}

static void
_kv_hash_complete_load(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct kv_hash_load_ctx *ctx = cb_arg;
	struct kv_hash_meta *meta_info = ctx->payload;
	int rc = 0;

	/* load data from payload */
	if (memcmp(meta_info->signature, KV_HASH_SIG,
				sizeof(meta_info->signature)) != 0)	{
		rc = -EILSEQ;
		goto load_failure;
	}

	rc = kv_hash_insert_conf(ctx->bdev->name, ctx->vbdev_name,
			ctx->meta_start_lba, meta_info->buck_size);
	if (rc)	{
		goto load_failure;
	}

	ctx->kv_bdev = calloc(1, sizeof(struct bdev_kv_hash));
	if (!ctx->kv_bdev) {
		rc = -ENOMEM;
		SPDK_ERRLOG("could not allocate kv bdev node.\n");
		kv_hash_remove_config(kv_hash_get_conf(ctx->bdev->name, ctx->vbdev_name));
		goto load_failure;
	}

	ctx->kv_bdev->bdev.name = ctx->vbdev_name;
	SPDK_NOTICELOG("vbdev name: %s\n", ctx->kv_bdev->bdev.name);
	/*if (!ctx->kv_bdev->bdev.name) {
		rc = -ENOMEM;
		SPDK_ERRLOG("could not allocate string for bdev name.\n");
		kv_hash_remove_config(kv_hash_get_conf(ctx->bdev->name, ctx->vbdev_name));
		free(ctx->kv_bdev);
		goto load_failure;
	}*/

	ctx->kv_bdev->max_bucket_num = meta_info->buck_size;
	ctx->kv_bdev->hash = calloc(meta_info->buck_size, sizeof(struct hash_node));
	if (!ctx->kv_bdev->hash) {
		rc = -ENOMEM;
		SPDK_ERRLOG("could not allocate hash.\n");
		kv_hash_remove_config(kv_hash_get_conf(ctx->bdev->name, ctx->vbdev_name));
		free(ctx->kv_bdev->bdev.name);
		free(ctx->kv_bdev);
		goto load_failure;
	}

	SPDK_NOTICELOG("Got %d buckets.(MAX: %d per page)\n", meta_info->buck_size, MAX_BUCKET_PER_PAGE);
	if (meta_info->buck_size > MAX_BUCKET_PER_PAGE)
		ctx->loaded_bucket += MAX_BUCKET_PER_PAGE;
	else
		ctx->loaded_bucket += meta_info->buck_size;


	SPDK_NOTICELOG("Loaded %d buckets.\n", ctx->loaded_bucket);

	for(uint32_t i = 0; i < ctx->loaded_bucket; ++i) {
		ctx->kv_bdev->hash[i] = meta_info->info[i];
	}

	if (ctx->loaded_bucket <= meta_info->buck_size) {
		/* Load additional metadata */
		if (meta_info->next_lba == ctx->meta_start_lba) {
			goto no_more_load;
		}

		rc = kv_hash_load_more(meta_info->next_lba, ctx);
		if (rc)
			goto more_load_failure;
	}
	else {
		goto no_more_load;
	}

	spdk_bdev_free_io(bdev_io);
	return;

no_more_load:
	spdk_put_io_channel(ctx->ch);
	spdk_bdev_close(ctx->desc);
	spdk_free(meta_info);

	rc = kv_hash_register_remain(ctx->bdev, ctx->kv_bdev);
	ctx->cb(ctx->cb_arg, rc);

	free(ctx);
	spdk_bdev_free_io(bdev_io);
	return;

more_load_failure:
	free(ctx->kv_bdev->hash);
	free(ctx->kv_bdev->bdev.name);
	free(ctx->kv_bdev);
	kv_hash_remove_config(kv_hash_get_conf(ctx->bdev->name, ctx->vbdev_name));
load_failure:
	ctx->cb(ctx->cb_arg, rc);
	spdk_put_io_channel(ctx->ch);
	spdk_bdev_close(ctx->desc);
	spdk_free(meta_info);
	free(ctx);
	spdk_bdev_free_io(bdev_io);
}

static int
kv_hash_load(struct spdk_bdev *base_bdev, const char *vbdev_name,
		uint64_t meta_start_lba, hash_load_completion_cb cb_fn, void *cb_arg)
{
	struct spdk_bdev_desc *desc;
	int rc;
	struct kv_hash_load_ctx *ctx;
	struct kv_hash_meta *meta_info;
	struct spdk_io_channel *ch;

	rc = spdk_bdev_open_ext(base_bdev->name, true, 
			vbdev_kv_hash_base_bdev_event_cb, NULL, &desc);
	if (rc) {
		SPDK_ERRLOG("could not open base bdev %s.\n", spdk_bdev_get_name(base_bdev));
		return rc;
	}
	ch = spdk_bdev_get_io_channel(desc);

	ctx = calloc(1, sizeof(struct kv_hash_load_ctx));
	if (!ctx) {
		spdk_bdev_close(desc);
		SPDK_ERRLOG("could not allocate struct kv_hash_load_ctx.\n");
		return -ENOMEM;
	}

	ctx->vbdev_name = strdup(vbdev_name);
	if (!ctx->vbdev_name) {
		free(ctx);
		spdk_bdev_close(desc);
		SPDK_ERRLOG("could not allocate string for vbdev name.\n");
		return -ENOMEM;
	}

	meta_info = spdk_zmalloc(sizeof(*meta_info), 0x1000, NULL,
			SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	if (!meta_info) {
		free(ctx->vbdev_name);
		free(ctx);
		spdk_bdev_close(desc);
		SPDK_ERRLOG("could not allocate meta info with spdk_zmalloc.\n");
		return -ENOMEM;
	}

	ctx->bdev = base_bdev;
	ctx->desc = desc;
	//ctx->vbdev_name = vbdev_name;
	ctx->ch = ch;
	ctx->payload = meta_info;
	ctx->cb = cb_fn;
	ctx->cb_arg = cb_arg;
	ctx->meta_num = 1;
	ctx->loaded_bucket = 0;
	ctx->meta_start_lba = meta_start_lba;

	return spdk_bdev_read_blocks(desc, ch, meta_info, meta_start_lba,
			(sizeof(*meta_info)/base_bdev->blocklen), _kv_hash_complete_load, ctx);
}

int
spdk_vbdev_kv_hash_load(const char *vbdev_name, const char *bdev_name,
		uint64_t meta_start_lba, hash_load_completion_cb cb_fn, void *cb_arg)
{
	struct spdk_bdev *bdev = NULL;
	struct kv_hash_config *conf;

	bdev = spdk_bdev_get_by_name(bdev_name);
	if (!bdev) {
		return -ENODEV;
	}

	conf = kv_hash_get_conf(bdev_name, vbdev_name);
	if (conf) {
		if(strcmp(vbdev_name, conf->vbdev_name) == 0) {
			SPDK_ERRLOG("kv hash bdev %s already exists.\n", vbdev_name);
		}
		if (strcmp(bdev_name, conf->bdev_name) == 0) {
			SPDK_ERRLOG("base bdev %s already claimed.\n", bdev_name);
		}
		return -EEXIST;
	}

	return kv_hash_load(bdev, vbdev_name, meta_start_lba, cb_fn, cb_arg);
}

static void
kv_hash_examine(struct spdk_bdev *bdev)
{
	kv_hash_register(bdev);

	spdk_bdev_module_examine_done(&kv_hash_if);
}

SPDK_LOG_REGISTER_COMPONENT(vbdev_kv_hash)

