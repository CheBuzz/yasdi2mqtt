#ifndef MQTT_CLIENT_H
#define MQTT_CLIENT_H

#include <stdint.h>
#include <stdbool.h>

bool mqtt_init(char *server, uint16_t port, char *ssl_cert, char *user, char *password, char *topic_prefix, int qos_level, char *client_id);
void mqtt_destroy();

extern void (*mc_set_request_cb)(const char *sn, const char *action, const char *channel, const char *payload);

bool mqtt_send(char *topic, char *payload);
bool mqtt_send_retained(char *topic, char *payload);
bool mqtt_send_raw(const char *topic, const char *payload, bool retained);

#endif
