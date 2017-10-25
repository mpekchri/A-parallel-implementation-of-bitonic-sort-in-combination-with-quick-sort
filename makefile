CC = gcc
FLAGS = -std=gnu11 -O3

all:
	$(CC) $(FLAGS) parallel.c -o bitonic -lpthread -lm

