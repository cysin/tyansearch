
/*
* Copyright (c) Z.G. Shi  <blueflycn at gmail dot com>
* Make a better world~
*/
#include <stdio.h>
#include "tyn_utils.h"

unsigned int tyn_hash32(char* str, unsigned int len)
{
   //this is fnv32 hash
   const unsigned int fnv_prime = 0x811C9DC5;
   unsigned int hash      = 0;
   unsigned int i         = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash *= fnv_prime;
      hash ^= (*str);
   }

   return hash;
}

int tyn_log(FILE *stream, char *format, ...) {
    va_list args;
    va_start (args, format);
    vfprintf (stream, format, args);
    fprintf(stream, "\n");
    va_end (args);
}
