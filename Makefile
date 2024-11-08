CC = gcc
CFLAGS = -Wall -Werror
LDFLAGS = -pthread 

OBJ = threadpool.o mapreduce.o distwc.o

all: wordcount

threadpool.o: threadpool.c
	$(CC) $(CFLAGS) -c threadpool.c -o threadpool.o

mapreduce.o: mapreduce.c
	$(CC) $(CFLAGS) -c mapreduce.c -o mapreduce.o

distwc.o: distwc.c
	$(CC) $(CFLAGS) -c distwc.c -o distwc.o

wordcount: $(OBJ)
	$(CC) $(OBJ) $(LDFLAGS) -o wordcount

clean:
	rm -f $(OBJ) wordcount

