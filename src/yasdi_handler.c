#include "yasdi_handler.h"
#include "log.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <libyasdimaster.h>

#define MAX_DRIVER_COUNT 128
#define MAX_CHANNEL_COUNT 300

/* Retry settings for requests arriving before device detection has finished (startup) */
#define DEVICE_WAIT_ATTEMPTS 12
#define DEVICE_WAIT_INTERVAL 5
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>

#define PARAM_CACHE_FMT "%s/%lu.channels.txt"
#define MAX_STAT_TEXT_COUNT 8

DWORD driver_count;
DWORD drivers[MAX_DRIVER_COUNT];
DWORD max_device_count;
DWORD *active_devices;
unsigned int update_interval;
char *cache_dir;

void (*yh_new_values_cb)(struct device_value_t *);

struct device_value_t **yh_get_values();
void device_detection_cb(TYASDIDetectionSub event, DWORD device);

static pthread_mutex_t yasdi_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 * Initializes yasdiMaster, enables the driver and prepares for device detection. Must be called first.
 * @param ini_file               relative location of yasdi.ini file
 * @param device_countmaximum    amount of devices that might be connected at the same time
 * @param device_update_interval seconds to wait between device value updates
 * @return true if initialization succeeded
 */
bool yh_init(const char *ini_file, DWORD device_count, unsigned int device_update_interval)
{
    int status;
    max_device_count = device_count;
    update_interval = device_update_interval;
    cache_dir = getenv("YASDI_CACHE_DIR");

    status = yasdiMasterInitialize(ini_file, &driver_count);
    if (status != 0)
    {
        log_fatal("Error while initializing yasdiMaster: %d", status);
        return false;
    }

    if (driver_count > MAX_DRIVER_COUNT)
    {
        log_warn("yasdi.ini enables more drivers (%u) than we can support (%u)", driver_count, MAX_DRIVER_COUNT);
        driver_count = MAX_DRIVER_COUNT;
    }

    yasdiMasterGetDriver(drivers, driver_count);

    for (int i=0; i < driver_count; ++i)
    {
        char driver_name[256] = "N/A";
        if (!yasdiMasterGetDriverName(drivers[i], driver_name, sizeof(driver_name)))
        {
            log_warn("Unable to get driver name for ID: %u", drivers[i]);
        }

        if (!yasdiMasterSetDriverOnline(drivers[i]))
        {
            log_fatal("Unable to set driver %s (ID: %u) online", driver_name, drivers[i]);
            return false;
        }
        else
        {
            log_info("Driver %s (ID: %u) online", driver_name, drivers[i]);
        }
    }

    // Prepare for device detection
    active_devices = calloc(max_device_count, sizeof(DWORD));
    yasdiMasterAddEventListener(device_detection_cb, YASDI_EVENT_DEVICE_DETECTION);

    // Authenticate at installer level so subsequent parameter detection/writes succeed
    yh_ensure_auth();

    return true;
}

/**
 * Shuts down yasdiMaster and frees related resources.
 */
void yh_destroy()
{
    yasdiMasterRemEventListener(device_detection_cb, YASDI_EVENT_DEVICE_DETECTION);
    for (int i=0; i < driver_count; ++i)
    {
        yasdiMasterSetDriverOffline(drivers[i]);
    }
    yasdiMasterShutdown();

    free(active_devices);
}

void yh_loop()
{
    for (;;)
    {
        time_t loop_start_time = time(NULL);

        // Count active devices
        DWORD active_device_count = 0;
        for (DWORD i = 0; i < max_device_count; i++)
        {
            if (active_devices[i] != 0)
                active_device_count++;
        }

        // Check for new devices if not all are online yet
        if (active_device_count != max_device_count)
        {
            log_debug("Not all devices are online, starting device detection (async)...");

            // yasdi is not thread-safe: serialize against set/get requests
            // arriving via the mqtt callback thread
            pthread_mutex_lock(&yasdi_mutex);
            int status = DoStartDeviceDetection(max_device_count, false);
            pthread_mutex_unlock(&yasdi_mutex);
            switch (status)
            {
            case YE_OK:
                break;
            case YE_NOT_ALL_DEVS_FOUND:
                log_debug("Unable to find all devices (target: %u)", max_device_count);
                break;
            case YE_DEV_DETECT_IN_PROGRESS:
                log_debug("Device detection already running, won't start new query");
                break;
            default:
                log_error("Unknown error while detecting devices: %d", status);
            }
        }

        // Collect inverter data and call cb for each
        pthread_mutex_lock(&yasdi_mutex);
        struct device_value_t **values = yh_get_values();
        pthread_mutex_unlock(&yasdi_mutex);
        for (DWORD i = 0; i < max_device_count; i++)
        {
            struct device_value_t *value = values[i];

            if (value != NULL)
            {
                (*yh_new_values_cb)(value);

                cJSON_Delete(value->json);
                free(value);
            }
        }
        free(values);

        int sleep_time = update_interval - difftime(time(NULL), loop_start_time);
        if (sleep_time < 0)
            sleep_time = 0;

        // Dirty fix to keep container responsive during sleep
        log_debug("yh_loop is going to sleep for %d seconds...", sleep_time);
        for (int i = 0; i < sleep_time; i++)
            sleep(1);
    }
}

struct device_value_t **yh_get_values()
{
    int status;
    struct device_value_t **values = calloc(max_device_count, sizeof(struct device_value_t *));

    log_debug("Starting device value fetch (sync)...");
    for (DWORD device_i = 0; device_i < max_device_count; device_i++)
    {
        DWORD device = active_devices[device_i];
        if (device == 0)
            continue;

        DWORD channels[MAX_CHANNEL_COUNT];
        DWORD channel_count = GetChannelHandlesEx(device, channels, MAX_CHANNEL_COUNT, SPOTCHANNELS);
        if (channel_count == 0)
        {
            log_debug("Device channel list download is apparently still in progress, skipping device %u...", device);
            continue;
        }

        DWORD device_sn;
        status = GetDeviceSN(device, &device_sn);
        switch (status)
        {
        case YE_OK:
            break;
        case YE_UNKNOWN_HANDLE:
            log_error("Unable to resolve sn of device %u", device);
            continue;
        default:
            log_error("Unknown error while detecting sn of device %u", device);
            continue;
        }

        // Prepare json output (format: {"sn": "...", "values": {...}})
        cJSON *device_json = cJSON_CreateObject();
        cJSON_AddNumberToObject(device_json, "sn", device_sn);
        cJSON_AddNumberToObject(device_json, "time", time(NULL));
        cJSON *device_values = cJSON_CreateObject();
        cJSON_AddItemToObject(device_json, "values", device_values);

        for (DWORD channel_i = 0; channel_i < channel_count; channel_i++)
        {
            DWORD channel = channels[channel_i];

            char channel_name[MAX_CHANNEL_NAME_SIZE];
            status = GetChannelName(channel, channel_name, MAX_CHANNEL_NAME_SIZE);
            switch (status)
            {
            case YE_OK:
                break;
            case YE_UNKNOWN_HANDLE:
                log_error("Unable to resolve name for device %u / channel %u", device, channel);
                continue;
            default:
                log_error("Unknown error while resolving name for device %u / channel %u: %d", device, channel, status);
                continue;
            }

            double value;
            char value_str[MAX_VALUE_STR_SIZE];
            status = GetChannelValue(channel, device, &value, value_str, MAX_VALUE_STR_SIZE, 0);
            switch (status)
            {
            case YE_OK:
                break;
            case YE_UNKNOWN_HANDLE:
                log_error("Unknown channel %u for device %u", channel, device);
                continue;
            case YE_TIMEOUT:
                // Device apparently went offline
                log_debug("Timeout for device %u (channel %u) while fetching value", device, channel);
                RemoveDevice(device);
                goto skip_device;
            case YE_VALUE_NOT_VALID:
                log_error("Got invalid value for device %u / channel %u", device, channel);
                continue;
            default:
                log_error("Unknown error while fetching value for device %u / channel %u: %d", device, channel, status);
                continue;
            }

            if (value_str[0] == '\0')
                cJSON_AddNumberToObject(device_values, channel_name, value);
            else
                cJSON_AddStringToObject(device_values, channel_name, value_str);
        }

        // Add sn and device json (including values) to array
        values[device_i] = malloc(sizeof(struct device_value_t));
        values[device_i]->device_sn = device_sn;
        values[device_i]->json = device_json;

        continue;

    skip_device:;
        // Clean up device json object if not added to array
        cJSON_Delete(device_json);
    }

    return values;
}

void device_detection_cb(TYASDIDetectionSub event, DWORD device)
{
    log_debug("Event %d fired for device %u", event, device);

    // Check if device is already active & find any free index if possible
    DWORD device_i, free_item_i;
    bool device_active = false, has_free_item = false;
    for (DWORD i = 0; i < max_device_count; i++)
    {
        if (active_devices[i] == device)
        {
            // Device is already active
            device_i = i;
            device_active = true;
        }
        else if (active_devices[i] == 0)
        {
            // Found free index
            free_item_i = i;
            has_free_item = true;
        }
    }

    switch (event)
    {
    case YASDI_EVENT_DEVICE_ADDED:
        log_info("Device %u went online", device);
        if (!device_active)
        {
            if (has_free_item)
                active_devices[free_item_i] = device;
            else
                log_error("Device count exceeds active_devices array size (size: %u)", max_device_count);
        }
        else
        {
            log_warn("Detected device %u is already part of active_devices", device);
        }
        break;
    case YASDI_EVENT_DEVICE_REMOVED:
        log_info("Device %u went offline", device);
        if (device_active)
            active_devices[device_i] = 0;
        else
            log_warn("Removed device %u is not part of active_devices", device);
        break;
    case YASDI_EVENT_DEVICE_SEARCH_END:
        log_debug("Device detection finished with %u devices", device);
        break;
    case YASDI_EVENT_DOWNLOAD_CHANLIST:
        log_debug("Downloading channel list for device %u", device);
        break;
    default:
        log_warn("Got invalid event (device: %u) for device_detection_cb: %d", device, event);
    }
}
/*
 * Installer-level access ('inst') is required for parameter writes.
 * Password scheme (same as yasdishell): "ID00<day + month + last 2 digits of year>"
 */
void yh_ensure_auth()
{
    static bool authenticated = false;
    if (authenticated)
        return;

    time_t now = time(NULL);
    struct tm *lt = localtime(&now);
    int datesum = lt->tm_mday + (lt->tm_mon + 1) + (lt->tm_year % 100);

    char passwd[12];
    snprintf(passwd, sizeof(passwd), "ID00%d", datesum);

    if (yasdiMasterSetAccessLevel("inst", passwd))
    {
        authenticated = true;
        log_info("Authenticated at installer level (password: %s)", passwd);
    }
    else
    {
        log_warn("Unable to authenticate at installer level (password: %s)", passwd);
    }
}

/* Build path of the channel cache file for a device sn. */
static void channel_cache_path(DWORD device_sn, char *path, size_t size)
{
    snprintf(path, size, PARAM_CACHE_FMT, cache_dir, (unsigned long)device_sn);
}

/* Write all parameter channel handles/names of a device to the cache file. */
static void write_param_channel_cache(DWORD device, DWORD device_sn)
{
    if (cache_dir == NULL)
        return;

    char path[256];
    channel_cache_path(device_sn, path, sizeof(path));

    FILE *file = fopen(path, "w");
    if (file == NULL)
    {
        log_warn("Unable to write channel cache file: %s", path);
        return;
    }

    DWORD channels[MAX_CHANNEL_COUNT];
    DWORD channel_count = GetChannelHandlesEx(device, channels, MAX_CHANNEL_COUNT, PARAMCHANNELS);
    for (DWORD i = 0; i < channel_count; i++)
    {
        char name[MAX_CHANNEL_NAME_SIZE];
        if (GetChannelName(channels[i], name, sizeof(name)) == YE_OK)
            fprintf(file, "%lu %s\n", (unsigned long)channels[i], name);
    }
    fclose(file);
    log_debug("Cached %u parameter channels of device %lu to %s", channel_count, (unsigned long)device_sn, path);
}

/*
 * Resolve a parameter channel handle by name from the CURRENT session's
 * channel list. Never use handles from the on-disk cache file: yasdi
 * reassigns channel handles on every start, so a cached handle can point
 * outside the current channel container - yasdi does not validate it, and
 * writing through it corrupts the heap (crash later inside free()).
 */
static DWORD find_param_channel(DWORD device, DWORD device_sn, const char *channel_name)
{
    DWORD channels[MAX_CHANNEL_COUNT];
    DWORD channel_count = GetChannelHandlesEx(device, channels, MAX_CHANNEL_COUNT, PARAMCHANNELS);
    if (channel_count == 0)
        return 0;

    write_param_channel_cache(device, device_sn);

    for (DWORD i = 0; i < channel_count; i++)
    {
        char name[MAX_CHANNEL_NAME_SIZE];
        if (GetChannelName(channels[i], name, sizeof(name)) == YE_OK && strcmp(name, channel_name) == 0)
            return channels[i];
    }
    return 0;
}

/*
 * Resolve a device handle by sn, waiting up to DEVICE_WAIT_ATTEMPTS * DEVICE_WAIT_INTERVAL
 * seconds for pending device detection if the device is not online yet. Queued mqtt
 * messages (clean session disabled) may arrive right after startup, before yasdi has
 * finished detecting the devices.
 */
static DWORD find_device_by_sn(DWORD device_sn)
{
    for (int attempt = 1; attempt <= DEVICE_WAIT_ATTEMPTS; attempt++)
    {
        pthread_mutex_lock(&yasdi_mutex);
        DWORD device = FindDeviceSN(device_sn);
        pthread_mutex_unlock(&yasdi_mutex);

        if (device != 0)
            return device;

        if (attempt < DEVICE_WAIT_ATTEMPTS)
        {
            log_debug("Device %lu is not online yet (attempt %d/%d), retrying in %d seconds...",
                      (unsigned long)device_sn, attempt, DEVICE_WAIT_ATTEMPTS, DEVICE_WAIT_INTERVAL);
            sleep(DEVICE_WAIT_INTERVAL);
        }
    }

    return 0;
}

bool yh_set_value(DWORD device_sn, const char *channel_name, const char *value_str)
{
    if (channel_name == NULL || value_str == NULL || value_str[0] == '\0')
        return false;

    DWORD device = find_device_by_sn(device_sn);
    if (device == 0)
    {
        log_error("Unable to find device with sn %lu", (unsigned long)device_sn);
        return false;
    }

    pthread_mutex_lock(&yasdi_mutex);

    DWORD channel = find_param_channel(device, device_sn, channel_name);
    if (channel == 0)
    {
        log_error("Unable to find parameter channel '%s' for device %lu", channel_name, (unsigned long)device_sn);
        pthread_mutex_unlock(&yasdi_mutex);
        return false;
    }

    yh_ensure_auth();

    // Auto-detect value type: numeric payload -> double setter, else string setter
    char *end;
    double dbl_value = strtod(value_str, &end);
    bool numeric = (end != value_str && *end == '\0');

    int status = numeric ? SetChannelValue(channel, device, dbl_value)
                         : SetChannelValueString(channel, device, value_str);

    // Retry once with fresh authentication on access-rights errors (-5)
    if (status == -5)
    {
        log_debug("Write to %s failed with access-rights error, re-authenticating...", channel_name);
        yh_ensure_auth();
        status = numeric ? SetChannelValue(channel, device, dbl_value)
                         : SetChannelValueString(channel, device, value_str);
    }

    pthread_mutex_unlock(&yasdi_mutex);

    if (status != 0)
        log_error("Unable to write channel '%s' of device %lu: status %d", channel_name, (unsigned long)device_sn, status);
    else
        log_info("Channel '%s' of device %lu set to '%s'", channel_name, (unsigned long)device_sn, value_str);

    return status == 0;
}

bool yh_get_param_value(DWORD device_sn, const char *channel_name, char *value_buf, size_t buf_size)
{
    if (channel_name == NULL || value_buf == NULL)
        return false;

    DWORD device = find_device_by_sn(device_sn);
    if (device == 0)
    {
        log_error("Unable to find device with sn %lu", (unsigned long)device_sn);
        return false;
    }

    pthread_mutex_lock(&yasdi_mutex);

    DWORD channel = find_param_channel(device, device_sn, channel_name);
    if (channel == 0)
    {
        log_error("Unable to find parameter channel '%s' for device %lu", channel_name, (unsigned long)device_sn);
        pthread_mutex_unlock(&yasdi_mutex);
        return false;
    }

    double value;
    int status = GetChannelValue(channel, device, &value, value_buf, buf_size, 0);

    pthread_mutex_unlock(&yasdi_mutex);

    if (status != YE_OK)
    {
        log_error("Unable to read parameter channel '%s' of device %lu: status %d", channel_name, (unsigned long)device_sn, status);
        return false;
    }

    // Numeric channels return an empty text buffer -> format the double value
    if (value_buf[0] == '\0')
        snprintf(value_buf, buf_size, "%g", value);

    return true;
}

void yh_foreach_param_channel(DWORD device_sn, yh_param_channel_cb cb)
{
    if (cb == NULL)
        return;

    pthread_mutex_lock(&yasdi_mutex);

    DWORD device = FindDeviceSN(device_sn);
    if (device == 0)
    {
        pthread_mutex_unlock(&yasdi_mutex);
        return;
    }

    DWORD channels[MAX_CHANNEL_COUNT];
    DWORD channel_count = GetChannelHandlesEx(device, channels, MAX_CHANNEL_COUNT, PARAMCHANNELS);
    if (channel_count > 0)
        write_param_channel_cache(device, device_sn);

    /*
     * ALL yasdi calls must happen under the mutex: yasdi has no internal
     * locking, and leaving the per-channel queries unlocked (as before) raced
     * with set/get requests on the mqtt thread and corrupted yasdi's internal
     * state (crash inside libc string/malloc code). Snapshot the channel info
     * here and invoke the callback after unlocking, since the callback
     * publishes via paho.
     */
    struct param_channel_info_t *infos = NULL;
    if (channel_count > 0)
        infos = calloc(channel_count, sizeof(struct param_channel_info_t));

    for (DWORD i = 0; infos != NULL && i < channel_count; i++)
    {
        struct param_channel_info_t *info = &infos[i];

        if (GetChannelName(channels[i], info->name, sizeof(info->name)) != YE_OK)
            continue; // name stays empty -> skipped below
        GetChannelUnit(channels[i], info->unit, sizeof(info->unit));

        int stat_cnt = GetChannelStatTextCnt(channels[i]);
        info->stat_text_cnt = stat_cnt > MAX_STAT_TEXT_COUNT ? MAX_STAT_TEXT_COUNT : stat_cnt;
        for (int t = 0; t < info->stat_text_cnt; t++)
            GetChannelStatText(channels[i], t, info->stat_texts[t], MAX_VALUE_STR_SIZE);
    }

    pthread_mutex_unlock(&yasdi_mutex);

    if (infos == NULL)
        return;

    for (DWORD i = 0; i < channel_count; i++)
    {
        if (infos[i].name[0] != '\0')
            cb(device_sn, &infos[i]);
    }

    free(infos);
}
