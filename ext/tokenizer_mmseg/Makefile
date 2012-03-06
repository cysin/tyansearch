CC=gcc
CPP=g++
CPPFLAGS=-Wall -fPIC -O3 -g -I../../

OBJS = tokenizer_mmseg.o 

.cpp:
	$(CPP) $(CPPFLAGS) $@.cpp -o $@

tokenizer_mmseg.so: $(OBJS)
	$(CPP) $(CPPFLAGS) $(OBJS) -shared -Wl,-soname,tokenizer_mmseg.so -o tokenizer_mmseg.so

clean:
	rm -f tokenizer_mmseg.so $(OBJS)
