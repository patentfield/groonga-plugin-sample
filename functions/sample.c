#include <string.h>
#include <stdlib.h>
#include <groonga/plugin.h>

#ifdef __GNUC__
# define GNUC_UNUSED __attribute__((__unused__))
#else
# define GNUC_UNUSED
#endif


typedef struct _cache_entry cache_entry;
struct _cache_entry {
  grn_id id;
  double data;
};

grn_hash *cache_keys = NULL;

grn_plugin_mutex *mutex = NULL;

static void
add_cache(grn_ctx *ctx, const char *key, unsigned int key_length)
{
  grn_id id;
  int added;
  cache_entry *entry = NULL;

  id = grn_hash_add(ctx, cache_keys,
                    key,
                    key_length,
                    (void **)&entry, &added);
  if (added) {
    entry->id = id;
    entry->data = 0.1;
  }
}

static cache_entry *
get_cache(grn_ctx *ctx, const char *key, unsigned int key_length)
{
  grn_id id;
  cache_entry *entry = NULL;

  id = grn_hash_get(ctx, cache_keys,
                    key,
                    key_length,
                    (void **)&entry);
  if (id != GRN_ID_NIL && entry) {
    return entry;
  }
}

static grn_rc
selector_sample(grn_ctx *ctx, GNUC_UNUSED grn_obj *table, GNUC_UNUSED grn_obj *index,
                GNUC_UNUSED int nargs, grn_obj **args,
                grn_obj *res, GNUC_UNUSED grn_operator op)
{
  grn_rc rc = GRN_SUCCESS;
  grn_obj *column_name;
  grn_obj *includes = NULL;
  grn_hash *include_keys = NULL;

  if (nargs > 3) {
    GRN_PLUGIN_ERROR(ctx, GRN_INVALID_ARGUMENT,
                     "sample_selector(): wrong number of arguments (%d for 1..2)",
                     nargs - 1);
    return GRN_INVALID_ARGUMENT;
  }
  column_name = args[1];

  if (args[2]->header.type == GRN_TABLE_HASH_KEY) {
    grn_hash_cursor *cursor;
    const char *key;
    grn_obj *value;
    unsigned int key_size;

    cursor = grn_hash_cursor_open(ctx, (grn_hash *)args[2],
                                  NULL, 0, NULL, 0,
                                  0, -1, 0);

    if (!cursor) {
      GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                       "sample_selector(): failed to open cursor for options");
      return ctx->rc;
    }
    while ((grn_hash_cursor_next(ctx, cursor)) != GRN_ID_NIL) {
      grn_hash_cursor_get_key_value(ctx, cursor, (void **)&key, &key_size,
                                    (void **)&value);
      if (key_size == 8 && !memcmp(key, "includes", 8)) {
        includes = value;
      } else {
        GRN_PLUGIN_ERROR(ctx, GRN_INVALID_ARGUMENT, "invalid option name: <%.*s>",
                         key_size, (char *)key);
        grn_hash_cursor_close(ctx, cursor);
        goto exit;
      }
    }
    grn_hash_cursor_close(ctx, cursor);
  }

  char randkey[100];
  sprintf(randkey, "%d", rand() % 10);
  cache_entry *entry = NULL;
  entry = get_cache(ctx, randkey, strlen(randkey));
  if (!entry) {
    grn_plugin_mutex_lock(ctx, mutex);
    entry = get_cache(ctx, randkey, strlen(randkey));
    if (!entry) {
      add_cache(ctx, randkey, strlen(randkey));
    }
    grn_plugin_mutex_unlock(ctx, mutex);
  }
  sleep(3);

exit :

  return rc;
}


grn_rc
GRN_PLUGIN_INIT(GNUC_UNUSED grn_ctx *ctx)
{
  mutex = grn_plugin_mutex_open(ctx);

  grn_plugin_mutex_lock(ctx, mutex);
  cache_keys = grn_hash_create(ctx,
                               NULL,
                               GRN_TABLE_MAX_KEY_SIZE,
                               sizeof(cache_entry),
                               GRN_OBJ_KEY_VAR_SIZE);

  grn_id id;
  int added;
  cache_entry *entry = NULL;

  id = grn_hash_add(ctx, cache_keys,
                    "cache_key",
                    strlen("cache_key"),
                    (void **)&entry, &added);
  if (added) {
    entry->id = id;
    entry->data = 0.1;
  }
  grn_plugin_mutex_unlock(ctx, mutex);

  return GRN_SUCCESS;
}

grn_rc
GRN_PLUGIN_REGISTER(grn_ctx *ctx)
{

  {
    grn_obj *selector_proc;

    selector_proc = grn_proc_create(ctx, "sample_selector", -1, GRN_PROC_FUNCTION,
                                    NULL, NULL, NULL, 0, NULL);
    grn_proc_set_selector(ctx, selector_proc, selector_sample);
    grn_proc_set_selector_operator(ctx, selector_proc, GRN_OP_NOP);
  }

  return ctx->rc;
}

grn_rc
GRN_PLUGIN_FIN(GNUC_UNUSED grn_ctx *ctx)
{
  if (mutex) {
    grn_plugin_mutex_lock(ctx, mutex);
    if (cache_keys) {
      GRN_HASH_EACH_BEGIN(ctx, cache_keys, cursor, id) {
        grn_hash_delete_by_id(ctx, cache_keys, id, NULL);
      } GRN_HASH_EACH_END(ctx, cursor);
      grn_hash_close(ctx, cache_keys);
      cache_keys = NULL;
    }

    grn_plugin_mutex_unlock(ctx, mutex);

    grn_plugin_mutex_close(ctx, mutex);
    mutex = NULL;
  }

  return GRN_SUCCESS;
}
