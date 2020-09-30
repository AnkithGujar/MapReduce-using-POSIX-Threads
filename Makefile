CC = gcc
CFLAGS = -c

all:
	$(CC) MapReduce.c -o MapReduce -lpthread

MapReduce: MapReduce.c
	$(CC) $(CFLAGS) MapReduce.c
	$(CC) MapReduce.c -o MapReduce -lpthread
	
clean:	
	rm MapReduce
