CC ?= gcc
CFLAGS ?= -O3 -march=native -pthread -std=c11 -Wall -Wextra
LDFLAGS ?= -pthread

TARGET := mem_perf
SRC := mem_perf.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET) $(LDFLAGS)

clean:
	rm -f $(TARGET)
