#define _POSIX_C_SOURCE 200809L

#include "mqtt_client.h"
#include "log.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <MQTTClient.h>

#define MQTT_KEEP_ALIVE_INTERVAL 20
#define MQTT_DISCONNECT_TIMEOUT 1000
#define MQTT_RECONNECT_INTERVAL 10
#define MQTT_SSL_VERSION MQTT_SSL_VERSION_TLS_1_2
#define MAX_SERVER_URL_SIZE 128
#define MAX_TOPIC_NAME_SIZE 128

char *topic_pfx;
int qos_lvl;
MQTTClient client;
MQTTClient_connectOptions options;
MQTTClient_SSLOptions ssl_options;

void (*mc_set_request_cb)(const char *sn, const char *action, const char *channel, const char *payload) = NULL;

bool mqtt_connect();
void mqtt_conn_lost_cb(void *context, char *cause);
int mqtt_msg_arrived_cb(void *context, char *topicName, int topicLen, MQTTClient_message *message);
static void subscribe_set_topics();
bool mqtt_send_raw(const char *topic, const char *payload, bool retained);

bool mqtt_init(char *server, uint16_t port, char *ssl_cert, char *user, char *password, char *topic_prefix, int qos_level, char *client_id)
{
    int status;
    topic_pfx = topic_prefix;
    qos_lvl = qos_level;

    options = (MQTTClient_connectOptions)MQTTClient_connectOptions_initializer;
    options.keepAliveInterval = MQTT_KEEP_ALIVE_INTERVAL;
    options.cleansession = false;

    if (user != NULL && password != NULL)
    {
        options.username = user;
        options.password = password;
    }

    if (ssl_cert != NULL)
    {
        log_debug("Setting up ssl for mqtt connection");

        ssl_options = (MQTTClient_SSLOptions)MQTTClient_SSLOptions_initializer;
        ssl_options.trustStore = ssl_cert;
        ssl_options.sslVersion = MQTT_SSL_VERSION;

        options.ssl = &ssl_options;
    }

    char url[MAX_SERVER_URL_SIZE];
    if (strlen(server) >= MAX_SERVER_URL_SIZE - 12)
    {
        log_fatal("MQTT server exceeds maximum length");
        return false;
    }
    snprintf(url, sizeof(url), "%s://%s:%u", options.ssl ? "ssl" : "tcp", server, port);

    status = MQTTClient_create(&client, url, client_id, MQTTCLIENT_PERSISTENCE_NONE, NULL);
    if (status != MQTTCLIENT_SUCCESS)
    {
        log_fatal("Error while creating MQTTClient instance: %d", status);
        return false;
    }

    status = MQTTClient_setCallbacks(client, NULL, mqtt_conn_lost_cb, mqtt_msg_arrived_cb, NULL);
    if (status != MQTTCLIENT_SUCCESS)
    {
        log_fatal("Error while setting MQTTClient callbacks: %d", status);
        return false;
    }

    return mqtt_connect();
}

bool mqtt_connect()
{
    int status;

    do
    {
        status = MQTTClient_connect(client, &options);

        switch (status)
        {
        case MQTTCLIENT_SUCCESS:
            log_info("Connection to mqtt broker established");
            subscribe_set_topics();
            return true;
        case 1:
            log_warn("Connection refused while connecting to mqtt broker: Unacceptable protocol version");
            return false;
        case 2:
            log_warn("Connection refused while connecting to mqtt broker: Identifier rejected");
            return false;
        case 3:
            // Continue connection attempts
            log_warn("Connection refused while connecting to mqtt broker: Server unavailable");
            break;
        case 4:
            log_warn("Connection refused while connecting to mqtt broker: Bad user name or password");
            return false;
        case 5:
            log_warn("Connection refused while connecting to mqtt broker: Not authorized");
            return false;
        default:
            log_warn("Unknown error while connecting to mqtt broker (status: %d). Going to retry in %d seconds...", status, MQTT_RECONNECT_INTERVAL);
        }

        sleep(MQTT_RECONNECT_INTERVAL);
    } while (status != MQTTCLIENT_SUCCESS);
}

static void subscribe_set_topics()
{
    char filter[MAX_TOPIC_NAME_SIZE];
    const char *actions[] = {"set", "get"};

    for (unsigned int i = 0; i < sizeof(actions) / sizeof(actions[0]); i++)
    {
        int len = snprintf(filter, sizeof(filter), "%s/+/%s/#", topic_pfx, actions[i]);
        if (len < 0 || (size_t)len >= sizeof(filter))
        {
            log_error("Topic filter exceeds maximum length");
            continue;
        }

        int status = MQTTClient_subscribe(client, filter, qos_lvl);
        if (status != MQTTCLIENT_SUCCESS)
            log_error("Unable to subscribe to %s (status: %d)", filter, status);
        else
            log_info("Subscribed to %s", filter);
    }
}

void mqtt_destroy()
{
    MQTTClient_disconnect(client, MQTT_DISCONNECT_TIMEOUT);
    MQTTClient_destroy(&client);
}

bool mqtt_send_raw(const char *topic, const char *payload, bool retained)
{
    MQTTClient_message msg = MQTTClient_message_initializer;
    msg.payload = (void *)payload;
    msg.payloadlen = strlen(payload);
    msg.qos = qos_lvl;
    msg.retained = retained;

    int status = MQTTClient_publishMessage(client, topic, &msg, NULL);
    if (status != MQTTCLIENT_SUCCESS)
    {
        log_error("Unable to publish via mqtt (status: %d)", status);
        return false;
    }

    return true;
}

static bool mqtt_send_topic(const char *topic, const char *payload, bool retained)
{
    char topic_with_pfx[MAX_TOPIC_NAME_SIZE];
    int len = snprintf(topic_with_pfx, sizeof(topic_with_pfx), "%s/%s", topic_pfx, topic);
    if (len < 0 || (size_t)len >= sizeof(topic_with_pfx))
    {
        log_error("Topic name exceeds maximum length");
        return false;
    }

    return mqtt_send_raw(topic_with_pfx, payload, retained);
}

bool mqtt_send(char *topic, char *payload)
{
    return mqtt_send_topic(topic, payload, false);
}

bool mqtt_send_retained(char *topic, char *payload)
{
    return mqtt_send_topic(topic, payload, true);
}

void mqtt_conn_lost_cb(void *context, char *cause)
{
    log_warn("Lost connection to mqtt broker: %s", cause);
    mqtt_connect();
}

int mqtt_msg_arrived_cb(void *context, char *topicName, int topicLen, MQTTClient_message *message)
{
    // Topic format: <prefix>/<device_sn>/<action>/<ChannelName>
    if (mc_set_request_cb != NULL && topicName != NULL && message != NULL)
    {
        size_t pfx_len = strlen(topic_pfx);
        const char *topic = topicName;

        if (topicLen > 0 && (size_t)topicLen < pfx_len)
            pfx_len = (size_t)topicLen;

        if (strncmp(topic, topic_pfx, pfx_len) == 0 && strlen(topic) > pfx_len)
        {
            const char *rest = topic + pfx_len + 1; // skip '/'
            char *rest_copy = strdup(rest);
            char *sn = strtok(rest_copy, "/");
            char *action = strtok(NULL, "/");
            char *channel = strtok(NULL, "");

            if (sn != NULL && action != NULL && channel != NULL && message->payloadlen > 0)
            {
                char *payload = malloc(message->payloadlen + 1);
                memcpy(payload, message->payload, message->payloadlen);
                payload[message->payloadlen] = '\0';

                (*mc_set_request_cb)(sn, action, channel, payload);
                free(payload);
            }
            free(rest_copy);
        }
    }

    MQTTClient_freeMessage(&message);
    MQTTClient_free(&topicName);
    return 1;
}
