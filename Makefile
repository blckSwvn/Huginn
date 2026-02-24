CC = gcc
CFLAGS = -no-pie -Wall -Wextra -O2 -fsanitize=address,undefined -g -luring
TARGET = Huginn

SRCS = main.c ./picohttpparser/picohttpparser.c
OBJS = $(SRCS:.c=.o)

# Default rule
all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -I./picohttpparser/pichohttpparser.c -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
