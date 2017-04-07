
CC = gcc
CFLAGS = -std=c99 -Wall -Wextra

ALL:test

test:test.c
	$(CC) $(CFLAGS) -o test test.c

.PHONY : clean
clean:
	$(RM) -f *.o test
