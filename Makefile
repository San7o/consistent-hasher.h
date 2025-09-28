# SPDX-License-Identifier: MIT

## --- Settings ---

CFLAGS=-Wall -Werror -Wpedantic -ggdb -std=c99
LDFLAGS=
CC=gcc

OUT_NAME=test
OBJ=test.o

## --- Commands ---

# --- Targets ---

all: $(OUT_NAME)

run: $(OUT_NAME)
	chmod +x $(OUT_NAME)
	./$(OUT_NAME)

$(OUT_NAME): $(OBJ)
	$(CC) $(OBJ) $(LDFLAGS) $(CLAGS) -o $(OUT_NAME)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm $(OBJ) 2>/dev/null || :

distclean:
	rm $(OUT_NAME) 2>/dev/null || :
