YASDI_PATH=yasdi
DESTDIR=/usr/local/bin

IDIRS=-I$(YASDI_PATH)/libs -I$(YASDI_PATH)/include -I$(YASDI_PATH)/core -I$(YASDI_PATH)/driver -I$(YASDI_PATH)/smalib -I$(YASDI_PATH)/master -I$(YASDI_PATH)/os
SRC=src/main.c src/yasdi_handler.c src/mqtt_client.c src/log.c

CC=gcc
CFLAGS=-std=c11 -O3 -DLOG_USE_COLOR $(IDIRS)
# -lpaho-mqtt3a: paho *async* client - thread-safe with built-in automatic
# reconnect. The sync client (libpaho-mqtt3cs) required hand-rolled reconnect
# threads that kept racing paho's internal state and crashing.
DEPS=-lyasdimaster -lcjson -lpaho-mqtt3a

yasdi2mqtt: $(SRC)
	$(CC) -o yasdi2mqtt $(SRC) $(CFLAGS) $(DEPS)

install: yasdi2mqtt
	cp yasdi2mqtt $(DESTDIR)
