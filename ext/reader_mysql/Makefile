CC=gcc
CFLAGS=-Wall -fPIC -O3 -g -std=c99 -I../../ $(shell mysql_config --cflags) $(shell mysql_config --libs)

OBJS = reader_mysql.o 

.c:
	$(CC)  $(CFLAGS) $@.c -o $@



reader_mysql.so: $(OBJS)
	$(CC) $(CFLAGS) $(MYSQL_FLAGS) $(OBJS) -shared -Wl,-soname,reader_mysql.so -o reader_mysql.so

clean:
	rm -f reader_mysql.so $(OBJS)
