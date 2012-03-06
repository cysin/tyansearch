CC=gcc 
CFLAGS=-Wall -O3 -g -std=c99 -rdynamic -ldl

INDEXER_OBJS = tyn_indexer.o tyn_utils.o libconfig/grammar.o libconfig/libconfig.o libconfig/scanctx.o libconfig/scanner.o libconfig/strbuf.o 

all: tyn_indexer

tyn_indexer: $(INDEXER_OBJS)
	$(CC) $(CFLAGS) $(INDEXER_OBJS) -o tyn_indexer

SUBDIRECTORIES = ext/reader_mysql ext/reader_json_pipe
.PHONY: $(SUBDIRECTORIES)

$(SUBDIRECTORIES):
	make -C $@


clean:
	rm -f tyn_indexer $(INDEXER_OBJS) reader_json_pipe.so reader_mysql.so

