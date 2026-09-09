#define _POSIX_C_SOURCE 200809L

#include "mqtt_client.h"
#include "log.h"

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <MQTTAsync.h>

#define MQTT_KEEP_ALIVE_INTERVAL 20
#define MQTT_DISCONNECT_TIMEOUT 1000
#define MQTT_SSL_VERSION MQTT_SSL_VERSION_TLS_1_2
#define MAX_SERVER_URL_SIZE 128
#define MAX_TOPIC_NAME_SIZE 128

char *topic_pfx;
int qos_lvl;
MQTTAsync client;
MQTTAsync_connectOptions conn_opts;
MQTTAsync_SSLOptions ssl_options;

// Defined in main.c: set once the client is actually connected so discovery
// publishes can't race a pending connect
extern bool mqtt_client_ready;

void (*mc_set_request_cb)(const char *sn, const char *action, const char *channel, const char *payload) = NULL;

/*
 * Requests from the message-arrived callback are only queued here and executed
 * by a dedicated worker thread: the callback runs on a paho thread and must
 * return quickly, while the yasdi set/get requests it triggers block for
 * seconds (authentication, device lookup, serial bus communication).
 */
struct mqtt_request
{
    char *rest;     // "sn/action/channel"; sn/action/channel point into it
    char *payload;  // NUL-terminated copy of the message payload
    char *sn;
    char *action;
    char *channel;
    struct mqtt_request *next;
};

/*
 * Unlike the sync client (MQTTClient), the async client (MQTTAsync) is
 * thread-safe: publishing from our threads is fine while paho processes
 * incoming packets and reconnects on its own threads - no serialization
 * needed, and no hand-rolled reconnect logic that could race paho's internal
 * state (that crashed twice: an invalid free(), then a SIGSEGV during
 * reconnect).
 */
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
static struct mqtt_request *queue_head = NULL;
static struct mqtt_request *queue_tail = NULL;
static pthread_t worker_thread;

static void *worker_main(void *arg);
static void enqueue_request(struct mqtt_request *req);

void mqtt_conn_lost_cb(void *context, char *cause);
static void mqtt_connected_cb(void *context, char *cause);
static void mqtt_connect_failure_cb(void *context, MQTTAsync_failureData *response);
static void mqtt_subscribe_failure_cb(void *context, MQTTAsync_failureData *response);
int mqtt_msg_arrived_cb(void *context, char *topicName, int topicLen, MQTTAsync_message *message);
static void subscribe_set_topics();
bool mqtt_send_raw(const char *topic, const char *payload, bool retained);

bool mqtt_init(char *server, uint16_t port, char *ssl_cert, char *user, char *password, char *topic_prefix, int qos_level, char *client_id)
{
    int status;
    topic_pfx = topic_prefix;
    qos_lvl = qos_level;

    conn_opts = (MQTTAsync_connectOptions)MQTTAsync_connectOptions_initializer;
    conn_opts.keepAliveInterval = MQTT_KEEP_ALIVE_INTERVAL;
    conn_opts.cleansession = false;

    /*
     * Let paho handle reconnects itself: it retries with exponential backoff
     * (also covering a failed initial connect) and re-establishes the session
     * without us touching the client from other threads. Manually re-connecting
     * to the sync client from a helper thread kept racing with paho's internal
     * teardown of the dead connection and crashed (SIGSEGV, thread 8).
     */
    conn_opts.automaticReconnect = 1;
    // Don't set the reconnect delay fields: older paho headers don't have
    // automaticReconnectDelayMin/Max, and the defaults (doubling backoff up to
    // 60 s) are fine for us
    conn_opts.connectTimeout = MQTT_KEEP_ALIVE_INTERVAL;
    conn_opts.onFailure = mqtt_connect_failure_cb;

    if (user != NULL && password != NULL)
    {
        conn_opts.username = user;
        conn_opts.password = password;
    }

    if (ssl_cert != NULL)
    {
        log_debug("Setting up ssl for mqtt connection");

        ssl_options = (MQTTAsync_SSLOptions)MQTTAsync_SSLOptions_initializer;
        ssl_options.trustStore = ssl_cert;
        ssl_options.sslVersion = MQTT_SSL_VERSION;

        conn_opts.ssl = &ssl_options;
    }

    char url[MAX_SERVER_URL_SIZE];
    if (strlen(server) >= MAX_SERVER_URL_SIZE - 12)
    {
        log_fatal("MQTT server exceeds maximum length");
        return false;
    }
    snprintf(url, sizeof(url), "%s://%s:%u", conn_opts.ssl ? "ssl" : "tcp", server, port);

    status = MQTTAsync_create(&client, url, client_id, MQTTCLIENT_PERSISTENCE_NONE, NULL);
    if (status != MQTTASYNC_SUCCESS)
    {
        log_fatal("Error while creating MQTTAsync instance: %d", status);
        return false;
    }

    status = MQTTAsync_setCallbacks(client, NULL, mqtt_conn_lost_cb, mqtt_msg_arrived_cb, NULL);
    if (status != MQTTASYNC_SUCCESS)
    {
        log_fatal("Error while setting MQTTAsync callbacks: %d", status);
        return false;
    }

    // (Re-)subscribe on every successful connect, including the automatic
    // ones after a connection loss (connect onSuccess would only fire once)
    status = MQTTAsync_setConnected(client, NULL, mqtt_connected_cb);
    if (status != MQTTASYNC_SUCCESS)
    {
        log_fatal("Error while setting MQTTAsync connected callback: %d", status);
        return false;
    }

    // Start the request worker before connecting: queued messages may arrive
    // immediately after the subscription (clean session disabled)
    status = pthread_create(&worker_thread, NULL, worker_main, NULL);
    if (status != 0)
    {
        log_fatal("Unable to start mqtt request worker thread (status: %d)", status);
        return false;
    }

    // Returns immediately; progress is reported via the callbacks and failed
    // attempts are retried automatically
    status = MQTTAsync_connect(client, &conn_opts);
    if (status != MQTTASYNC_SUCCESS)
    {
        log_fatal("Unable to start mqtt connection (status: %d)", status);
        return false;
    }

    return true;
}

/*
 * Called by paho after every successful connect, including the automatic
 * reconnects after a connection loss.
 */
static void mqtt_connected_cb(void *context, char *cause)
{
    (void)context;
    (void)cause;
    log_info("Connection to mqtt broker established");
    mqtt_client_ready = true;
    subscribe_set_topics();
}

static void mqtt_connect_failure_cb(void *context, MQTTAsync_failureData *response)
{
    (void)context;
    log_warn("Unable to connect to mqtt broker (code: %d, message: %s), automatic reconnect in progress",
             response != NULL ? response->code : 0,
             response != NULL && response->message != NULL ? response->message : "N/A");
}

static void mqtt_subscribe_failure_cb(void *context, MQTTAsync_failureData *response)
{
    (void)context;
    log_error("Subscription was not acknowledged (code: %d)",
              response != NULL ? response->code : 0);
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

        MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
        opts.onFailure = mqtt_subscribe_failure_cb;

        int status = MQTTAsync_subscribe(client, filter, qos_lvl, &opts);
        if (status != MQTTASYNC_SUCCESS)
            log_error("Unable to subscribe to %s (status: %d)", filter, status);
        else
            log_info("Subscribing to %s", filter);
    }
}

void mqtt_destroy()
{
    // Only ever call this from the main thread (never from paho callbacks)
    MQTTAsync_disconnectOptions opts = MQTTAsync_disconnectOptions_initializer;
    opts.timeout = MQTT_DISCONNECT_TIMEOUT;
    MQTTAsync_disconnect(client, &opts);
    MQTTAsync_destroy(&client);
}

bool mqtt_send_raw(const char *topic, const char *payload, bool retained)
{
    MQTTAsync_message msg = MQTTAsync_message_initializer;
    MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;

    msg.payload = (void *)payload;
    msg.payloadlen = (int)strlen(payload);
    msg.qos = qos_lvl;
    msg.retained = retained;

    // Thread-safe on the async client: fine to publish while paho processes
    // incoming packets or reconnects on its own threads
    int status = MQTTAsync_sendMessage(client, topic, &msg, &opts);
    if (status != MQTTASYNC_SUCCESS)
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

/*
 * Runs on a paho thread whenever the connection is lost. Do NOT call connect
 * (or any blocking paho function) from here: with the sync client this raced
 * paho's internal connection teardown and crashed (SIGSEGV right after the
 * "Lost connection" log). automaticReconnect lets paho re-establish the
 * session itself; mqtt_connected_cb then resubscribes.
 */
void mqtt_conn_lost_cb(void *context, char *cause)
{
    (void)context;
    mqtt_client_ready = false;
    log_warn("Lost connection to mqtt broker: %s (automatic reconnect in progress)", cause != NULL ? cause : "unknown");
}

static void enqueue_request(struct mqtt_request *req)
{
    req->next = NULL;

    pthread_mutex_lock(&queue_mutex);
    if (queue_tail != NULL)
        queue_tail->next = req;
    else
        queue_head = req;
    queue_tail = req;
    pthread_cond_signal(&queue_cond);
    pthread_mutex_unlock(&queue_mutex);
}

static void *worker_main(void *arg)
{
    (void)arg;

    for (;;)
    {
        pthread_mutex_lock(&queue_mutex);
        while (queue_head == NULL)
            pthread_cond_wait(&queue_cond, &queue_mutex);
        struct mqtt_request *req = queue_head;
        queue_head = req->next;
        if (queue_head == NULL)
            queue_tail = NULL;
        pthread_mutex_unlock(&queue_mutex);

        if (mc_set_request_cb != NULL)
            (*mc_set_request_cb)(req->sn, req->action, req->channel, req->payload);

        free(req->rest);
        free(req->payload);
        free(req);
    }
    return NULL;
}

int mqtt_msg_arrived_cb(void *context, char *topicName, int topicLen, MQTTAsync_message *message)
{
    // Topic format: <prefix>/<device_sn>/<action>/<ChannelName>
    if (topicName != NULL && message != NULL && message->payloadlen > 0)
    {
        // paho does not NUL-terminate the topic when topicLen > 0
        size_t topic_len = topicLen > 0 ? (size_t)topicLen : strlen(topicName);
        char *topic = malloc(topic_len + 1);
        if (topic != NULL)
        {
            memcpy(topic, topicName, topic_len);
            topic[topic_len] = '\0';

            size_t pfx_len = strlen(topic_pfx);
            if (strncmp(topic, topic_pfx, pfx_len) == 0 && topic_len > pfx_len + 1)
            {
                struct mqtt_request *req = calloc(1, sizeof(*req));
                if (req != NULL)
                {
                    req->rest = strdup(topic + pfx_len + 1); // skip '/'
                    req->payload = malloc(message->payloadlen + 1);
                    if (req->rest != NULL && req->payload != NULL)
                    {
                        memcpy(req->payload, message->payload, message->payloadlen);
                        req->payload[message->payloadlen] = '\0';

                        req->sn = strtok(req->rest, "/");
                        req->action = strtok(NULL, "/");
                        req->channel = strtok(NULL, "");
                    }

                    if (req->rest == NULL || req->payload == NULL ||
                        req->sn == NULL || req->action == NULL || req->channel == NULL)
                    {
                        free(req->rest);
                        free(req->payload);
                        free(req);
                    }
                    else
                        enqueue_request(req);
                }
            }
            free(topic);
        }
    }

    // Async client: the application must free the message and topic before
    // returning (thread-safe, runs on a paho thread)
    MQTTAsync_freeMessage(&message);
    MQTTAsync_free(topicName);
    return 1;
}
