#ifndef YASDI_HANDLER_H
#define YASDI_HANDLER_H

#include <stdbool.h>
#include <stddef.h>
#include <cjson/cJSON.h>
#include <smadef.h>

#define MAX_CHANNEL_NAME_SIZE 20
#define MAX_VALUE_STR_SIZE 64

/**
 * Represents a value snapshot of a single device.
 */
struct device_value_t
{
    DWORD device_sn;
    cJSON *json;
};

extern void (*yh_new_values_cb)(struct device_value_t *);

/**
 * Info about a single parameter channel (used for HA discovery).
 */
struct param_channel_info_t
{
    char name[MAX_CHANNEL_NAME_SIZE];
    char unit[16];
    int stat_text_cnt;
    char stat_texts[8][MAX_VALUE_STR_SIZE];
};

typedef void (*yh_param_channel_cb)(DWORD device_sn, struct param_channel_info_t *info);

bool yh_init(const char *ini_file, DWORD device_count, unsigned int device_update_interval);
void yh_destroy();
void yh_loop();

/**
 * Writes a value (numeric or string payload) to a parameter channel of a device.
 * Authenticates at installer level automatically. Thread-safe.
 * @return true if the channel was written successfully
 */
bool yh_set_value(DWORD device_sn, const char *channel_name, const char *value_str);

/**
 * Reads the current value of a parameter channel of a device (as string).
 * Thread-safe.
 * @return true if the value could be read
 */
bool yh_get_param_value(DWORD device_sn, const char *channel_name, char *value_buf, size_t buf_size);

/**
 * Iterates over all parameter channels of a device and invokes cb for each.
 * Writes the channel list to the on-disk cache (YASDI_CACHE_DIR) for reference.
 */
void yh_foreach_param_channel(DWORD device_sn, yh_param_channel_cb cb);

/**
 * Authenticates at installer level ('inst') if not already done.
 */
void yh_ensure_auth();

#endif
