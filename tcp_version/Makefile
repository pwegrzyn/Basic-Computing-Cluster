CC := gcc
CFLAGS := -Wall -std=c99 -g
LDLIBS := -lpthread

all: clean client server

server: server.o
	$(CC) $(CFLAGS) -o server server.o $(LDLIBS)

server.o: server.c
	$(CC) $(CFLAGS) -c server.c

client: client.o
	$(CC) $(CFLAGS) -o client client.o $(LDLIBS)

client.o: client.c
	$(CC) $(CFLAGS) -c client.c

clean: 
	rm -f *.o client server