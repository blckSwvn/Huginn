CC = gcc
CFLAGS = -no-pie -Wall -Wextra -O2 -fsanitize=address,undefined -g -luring
TARGET = Huginn

SRCS = main.c
OBJS = $(SRCS:.c=.o)

# Default rule
all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
