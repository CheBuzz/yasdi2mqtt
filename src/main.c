#define _GNU_SOURCE // for pthread_setattr_default_np

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include <cjson/cJSON.h>
#include "yasdi_handler.h"
#include "mqtt_client.h"
#include "log.h"

#define HA_DISCOVERY_PREFIX_ENV "MQTT_DISCOVERY_PREFIX"
#define HA_DISCOVERY_PREFIX_DEFAULT "homeassistant"

/*
 * musl (alpine) creates threads with a 128 KiB stack by default (glibc: 8 MiB).
 * yasdi's async device detection thread overflows that stack and crashes
 * silently, so raise the process-wide default before any threads are spawned.
 */
#define THREAD_STACK_SIZE (8 * 1024 * 1024)

char *get_required_env(const char *name);
void new_values_cb(struct device_value_t *values);
void set_request_cb(const char *sn, const char *action, const char *channel, const char *payload);
void discovery_cb(DWORD device_sn, struct param_channel_info_t *info);
void publish_discovery(DWORD device_sn);

bool mqtt_set_enabled = false;
char *ha_discovery_prefix;
bool mqtt_client_ready = false;

static void raise_default_thread_stack_size()
{
    pthread_attr_t attr;
    int status = pthread_attr_init(&attr);
    if (status != 0)
    {
        log_warn("Unable to initialize thread attributes (status: %d)", status);
        return;
    }

    pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE);
    status = pthread_setattr_default_np(&attr);
    pthread_attr_destroy(&attr);

    if (status != 0)
        log_warn("Unable to raise default thread stack size to %d bytes (status: %d)", THREAD_STACK_SIZE, status);
    else
        log_info("Default thread stack size raised to %d bytes", THREAD_STACK_SIZE);
}

int main(int argc, char **argv)
{
    log_set_level(LOG_INFO);

    // Before paho/yasdi spawn any threads: bigger default stacks for musl
    raise_default_thread_stack_size();

    char *yasdi_config = get_required_env("YASDI_CONFIG");
    DWORD yasdi_max_device_count = strtoul(get_required_env("YASDI_MAX_DEVICE_COUNT"), NULL, 10);
    unsigned int yasdi_update_interval = strtoul(get_required_env("YASDI_UPDATE_INTERVAL"), NULL, 10);
    char *mqtt_topic_prefix = get_required_env("MQTT_TOPIC_PREFIX");
    char *mqtt_server = get_required_env("MQTT_SERVER");
    uint16_t mqtt_port = strtoul(get_required_env("MQTT_PORT"), NULL, 10);
    char *mqtt_ssl_cert = getenv("MQTT_SSL_CERT");
    char *mqtt_user = getenv("MQTT_USER");
    char *mqtt_password = getenv("MQTT_PASSWORD");

    mqtt_set_enabled = getenv("MQTT_SET_ENABLED") != NULL && atoi(getenv("MQTT_SET_ENABLED")) != 0;
    ha_discovery_prefix = getenv(HA_DISCOVERY_PREFIX_ENV);
    if (ha_discovery_prefix == NULL)
        ha_discovery_prefix = HA_DISCOVERY_PREFIX_DEFAULT;

    char *mqtt_client_id = "yasdi2mqtt";
    if (getenv("MQTT_CLIENT_ID"))
    {
        mqtt_client_id = getenv("MQTT_CLIENT_ID");
    }

    int mqtt_qos_level = 2;
    if (getenv("MQTT_QOS_LEVEL"))
    {
        mqtt_qos_level = strtol(getenv("MQTT_QOS_LEVEL"), NULL, 10);
    }

    if (getenv("LOG_LEVEL"))
    {
        log_set_level(strtol(getenv("LOG_LEVEL"), NULL, 10));
    }

    log_info("Configuration | YASDI_CONFIG = %s", yasdi_config);
    log_info("Configuration | YASDI_MAX_DEVICE_COUNT = %u", yasdi_max_device_count);
    log_info("Configuration | YASDI_UPDATE_INTERVAL = %u", yasdi_update_interval);
    log_info("Configuration | MQTT_TOPIC_PREFIX = %s", mqtt_topic_prefix);
    log_info("Configuration | MQTT_SERVER = %s", mqtt_server);
    log_info("Configuration | MQTT_PORT = %u", mqtt_port);
    log_info("Configuration | MQTT_SSL_CERT = %s", mqtt_ssl_cert);
    log_info("Configuration | MQTT_CLIENT_ID = %s", mqtt_client_id);
    log_info("Configuration | MQTT_QOS_LEVEL = %d", mqtt_qos_level);
    log_info("Configuration | MQTT_USER = %s", mqtt_user);
    log_info("Configuration | MQTT_PASSWORD = %s", mqtt_password);
    log_info("Configuration | MQTT_SET_ENABLED = %d", mqtt_set_enabled);
    log_info("Configuration | HA_DISCOVERY_PREFIX = %s", ha_discovery_prefix);

    if (!mqtt_init(mqtt_server, mqtt_port, mqtt_ssl_cert, mqtt_user, mqtt_password, mqtt_topic_prefix, mqtt_qos_level, mqtt_client_id))
    {
        log_fatal("Unable to initialize mqtt_client");
        return -1;
    }
    // mqtt_client_ready is set by mqtt_client once the connection is actually
    // established, so discovery publishes can't race a pending connect

    if (!yh_init(yasdi_config, yasdi_max_device_count, yasdi_update_interval))
    {
        log_fatal("Unable to initialize yasdi_handler");
        return -1;
    }

    yh_new_values_cb = new_values_cb;
    mc_set_request_cb = set_request_cb;
    yh_loop();

    return 0;
}

char *get_required_env(const char *name)
{
    if (!getenv(name))
    {
        log_fatal("Mandatory environment variable %s missing", name);
        exit(-1);
    }

    return getenv(name);
}

void new_values_cb(struct device_value_t *values)
{
    char topic[24];
    snprintf(topic, sizeof(topic), "%lu", (unsigned long)values->device_sn);
    char *json = cJSON_PrintUnformatted(values->json);

    mqtt_send(topic, json);
    free(json);

    // Publish HA discovery entities for the parameter channels (once per device)
    publish_discovery(values->device_sn);
}

void set_request_cb(const char *sn, const char *action, const char *channel, const char *payload)
{
    char topic[160];
    char result[MAX_VALUE_STR_SIZE * 2];

    DWORD device_sn = strtoul(sn, NULL, 10);

    if (!mqtt_set_enabled)
    {
        log_warn("Ignoring %s request for %s/%s: parameter writes are disabled (set MQTT_SET_ENABLED=1 to enable)", action, sn, channel);
        snprintf(topic, sizeof(topic), "%s/result/%s", sn, channel);
        snprintf(result, sizeof(result), "ERROR: parameter writes are disabled (MQTT_SET_ENABLED)");
        mqtt_send(topic, result);
        return;
    }

    if (strcmp(action, "set") == 0)
    {
        bool ok = yh_set_value(device_sn, channel, payload);
        snprintf(topic, sizeof(topic), "%s/result/%s", sn, channel);
        snprintf(result, sizeof(result), ok ? "OK" : "ERROR: channel was not written (see logs for status)");
        mqtt_send(topic, result);
    }
    else if (strcmp(action, "get") == 0)
    {
        char value[MAX_VALUE_STR_SIZE];
        bool ok = yh_get_param_value(device_sn, channel, value, sizeof(value));
        snprintf(topic, sizeof(topic), "%s/get/%s", sn, channel);
        if (ok)
            snprintf(result, sizeof(result), "%s", value);
        else
            snprintf(result, sizeof(result), "ERROR: unable to read channel");
        mqtt_send(topic, result);
    }
    else
    {
        log_warn("Ignoring unknown action '%s' for %s/%s", action, sn, channel);
        snprintf(topic, sizeof(topic), "%s/result/%s", sn, channel);
        snprintf(result, sizeof(result), "ERROR: unknown action '%s'", action);
        mqtt_send(topic, result);
    }
}

void publish_discovery(DWORD device_sn)
{
    static DWORD discovered[16] = {0};
    for (unsigned int i = 0; i < sizeof(discovered) / sizeof(discovered[0]); i++)
    {
        if (discovered[i] == 0)
        {
            discovered[i] = device_sn;
            break;
        }
        if (discovered[i] == device_sn)
            return; // already published
    }

    yh_foreach_param_channel(device_sn, discovery_cb);
}

void discovery_cb(DWORD device_sn, struct param_channel_info_t *info)
{
    const char *component = "text";
    cJSON *config = cJSON_CreateObject();
    char object_id[64], node_id[32], state_topic[128], command_topic[128];
    char topic[192];

    snprintf(node_id, sizeof(node_id), "yasdi2mqtt_%lu", (unsigned long)device_sn);
    snprintf(object_id, sizeof(object_id), "%s_%s", node_id, info->name);
    snprintf(state_topic, sizeof(state_topic), "%lu/get/%s", (unsigned long)device_sn, info->name);
    snprintf(command_topic, sizeof(command_topic), "%lu/set/%s", (unsigned long)device_sn, info->name);

    cJSON_AddStringToObject(config, "name", info->name);
    cJSON_AddStringToObject(config, "state_topic", state_topic);
    cJSON_AddStringToObject(config, "object_id", object_id);
    cJSON_AddStringToObject(config, "unique_id", object_id);

    if (info->stat_text_cnt > 0)
    {
        // Enum-type parameter -> select with the status texts as options
        component = "select";
        cJSON *options = cJSON_CreateArray();
        for (int i = 0; i < info->stat_text_cnt; i++)
            cJSON_AddItemToArray(options, cJSON_CreateString(info->stat_texts[i]));
        cJSON_AddItemToObject(config, "options", options);
        cJSON_AddStringToObject(config, "command_topic", command_topic);
    }
    else
    {
        // Numeric parameter -> number entity, everything else -> text entity
        component = "number";
        cJSON_AddStringToObject(config, "command_topic", command_topic);
        cJSON_AddNumberToObject(config, "step", 0.1);
        if (info->unit[0] != '\0' && strcmp(info->unit, "---") != 0)
            cJSON_AddStringToObject(config, "unit_of_measurement", info->unit);
    }

    char *payload = cJSON_PrintUnformatted(config);
    snprintf(topic, sizeof(topic), "%s/%s/%s/%s/config", ha_discovery_prefix, component, node_id, object_id);
    if (mqtt_client_ready)
        mqtt_send_raw(topic, payload, true);
    else
        log_warn("Skipping discovery publish for %s: mqtt not ready", info->name);
    free(payload);
    cJSON_Delete(config);
}
