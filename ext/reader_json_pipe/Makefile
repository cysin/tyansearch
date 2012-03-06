CC=gcc
CFLAGS=-Wall -fPIC -O3 -g -std=c99 -I../../

OBJS = reader_json_pipe.o yajl/yajl_alloc.o yajl/yajl_buf.o  yajl/yajl.o  yajl/yajl_encode.o  yajl/yajl_gen.o  yajl/yajl_lex.o  yajl/yajl_parser.o yajl/yajl_tree.o  yajl/yajl_version.o

reader_json_pipe.so: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -shared -Wl,-soname,reader_json_pipe.so -o reader_json_pipe.so

clean:
	rm -f reader_json_pipe.so $(OBJS)
