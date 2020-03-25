#include <string.h>
#include <stdlib.h>
#include <groonga/plugin.h>

#ifdef __GNUC__
# define GNUC_UNUSED __attribute__((__unused__))
#else
# define GNUC_UNUSED
#endif

static grn_obj *
func_strlen(grn_ctx *ctx, GNUC_UNUSED int nargs, GNUC_UNUSED grn_obj **args,
            grn_user_data *user_data)
{
  grn_obj *result;
  unsigned int str_length = GRN_TEXT_LEN(args[0]);

  if ((result = grn_plugin_proc_alloc(ctx, user_data, GRN_DB_INT64, 0))) {
    GRN_INT64_SET(ctx, result, str_length);
  }
  return result;
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

  include_keys = grn_hash_create(ctx, NULL,
                                 GRN_TABLE_MAX_KEY_SIZE,
                                 0,
                                 GRN_OBJ_TABLE_HASH_KEY|GRN_OBJ_KEY_VAR_SIZE);
  if (!include_keys) {
    GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                     "sample_selector(): failed to create include keys table");
    goto exit;
  }

  switch (includes->header.type) {
  case GRN_BULK :
    {
      const char *top, *cursor, *end;
      top = GRN_TEXT_VALUE(includes);
      cursor = top;
      end = top + GRN_TEXT_LEN(includes);
      for (; cursor <= end; cursor++) {
        if (cursor[0] == ',' || cursor == end) {
          grn_hash_add(ctx, include_keys, top, cursor - top, NULL, NULL);
          top = cursor + 1;
        }
      }
    }
    break;
  case GRN_VECTOR :
    {
      /* implement me */
    }
    break;
  default :
    break;
  }

  if (include_keys) {
    grn_obj buf;
    grn_obj *accessor;

    GRN_VOID_INIT(&buf);

    accessor = grn_obj_column(ctx, res,
                              GRN_TEXT_VALUE(column_name),
                               GRN_TEXT_LEN(column_name));
    if (!accessor) {
      GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                       "sample_selector(): can't open column <%.*s>",
                       (int32_t)GRN_TEXT_LEN(column_name),
                       GRN_TEXT_VALUE(column_name));
      goto exit;
    }

    GRN_HASH_EACH_BEGIN(ctx, (grn_hash *)res, cur, id) {
      GRN_BULK_REWIND(&buf);
      grn_obj_get_value(ctx, accessor, id, &buf);
      if (grn_hash_get(ctx, include_keys, GRN_TEXT_VALUE(&buf), GRN_TEXT_LEN(&buf), NULL) == GRN_ID_NIL) {
        grn_hash_cursor_delete(ctx, cur, NULL);
      }
    } GRN_HASH_EACH_END(ctx, cur);

    grn_obj_close(ctx, accessor);
    GRN_OBJ_FIN(ctx, &buf);
  }

exit :

  if (include_keys) {
    grn_hash_close(ctx, include_keys);
  }

  return rc;
}

grn_rc
GRN_PLUGIN_INIT(GNUC_UNUSED grn_ctx *ctx)
{
  return GRN_SUCCESS;
}

grn_rc
GRN_PLUGIN_REGISTER(grn_ctx *ctx)
{
  grn_proc_create(ctx, "strlen", -1, GRN_PROC_FUNCTION,
                  func_strlen, NULL, NULL, 0, NULL);


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
  return GRN_SUCCESS;
}
