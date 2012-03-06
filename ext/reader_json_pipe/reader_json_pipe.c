
/*
* Copyright (c) Z.G. Shi  <blueflycn at gmail dot com>
* Copyright (c) TYANSEARCH
* Make a better world~
*/

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>

#include "tyan.h"
#include "ext/reader_json_pipe/yajl/api/yajl_parse.h"
#include "ext/reader_json_pipe/yajl/api/yajl_gen.h"

typedef struct tyn_reader_context_t {
    char *buffer;
    size_t buffer_size;
    FILE *json_stream;
    size_t bytes_consumed;
    size_t bytes_read;
    int parse_state;
    yajl_handle handle;
    int depth;
    int state;
    char *key;
    size_t key_length;
    tyn_document_t *document;
} tyn_reader_context_t;


int yajl_null(tyn_reader_context_t *context)
{
    if(context->state ==1) {
        free(context->key);
    }
    context->state = 0;
    return 1;
}

int yajl_boolean(tyn_reader_context_t *context, int boolVal)
{
    if(context->state ==1) {
        free(context->key);
    }
    context->state = 0;
    return 1;
}

int yajl_integer(tyn_reader_context_t *context, long long integerVal)
{
    if(context->state ==1) {
        free(context->key);
    }
    context->state = 0;
    return 1;
}

int yajl_double(tyn_reader_context_t *context, double doubleVal)
{
    if(context->state ==1) {
        free(context->key);
    }
    context->state = 0;
    return 1;
}

int yajl_string(tyn_reader_context_t *context, const unsigned char * stringVal, size_t stringLen) {
    if(context->state == 1) {
        char *value = (char *)malloc(stringLen);
        memcpy(value, stringVal, stringLen);
        tyn_document_t *document = context->document;
        *(document->field_names + document->num_fields) = context->key;
        *(document->field_name_lengths + document->num_fields) = context->key_length;
        *(document->fields + document->num_fields) = value;
        *(document->field_lengths + document->num_fields) = stringLen;
        //fwrite(context->key, 1, context->key_length, stdout);
        //printf("\n");
        //fwrite(value, 1, stringLen, stdout);
        //printf("\n");
        document->num_fields++;
    }
    
    context->state = 0;
    return 1;
}

int yajl_map_key(tyn_reader_context_t *context, const unsigned char * stringVal, size_t stringLen) {
    if(context->depth == 2) {
        context->state = 1;
        char *key = (char *)malloc(stringLen);
        memcpy(key, stringVal, stringLen);
        context->key = key;
        context->key_length = stringLen;
    }
    return 1;
}

int yajl_start_map(tyn_reader_context_t *context)
{
    context->depth++;
    if(context->state ==1) {
        free(context->key);
    }
    context->state = 0;
    //printf("map open '{'\n");
    return 1;
}


int yajl_end_map(tyn_reader_context_t *context)
{
    context->depth--;
    if(context->state ==1) {
        free(context->key);
    }

    context->state = 0;
    if(context->depth == 1) {
        context->parse_state = 1;
        return 0;
    }
    //printf("map close '}'\n");
    return 1;
}

int yajl_start_array(tyn_reader_context_t *context)
{
    context->depth++;
    if(context->state ==1) {
        free(context->key);
    }

    context->state = 0;
    //printf("array open '['\n");
    return 1;
}

int yajl_end_array(tyn_reader_context_t *context)
{
    context->depth--;
    if(context->state ==1) {
        free(context->key);
    }
    context->state = 0;
   // printf("array close ']'\n");
    return 1;
}

static yajl_callbacks callbacks = {
    yajl_null,
    yajl_boolean,
    yajl_integer,
    yajl_double,
    NULL,//yajl_number,
    yajl_string,
    yajl_start_map,
    yajl_map_key,
    yajl_end_map,
    yajl_start_array,
    yajl_end_array
};

tyn_document_t* tyn_reader_fetch_document(tyn_reader_t *reader) {
    tyn_reader_context_t *context = (tyn_reader_context_t *)reader->context;
    context->document = reader->document;
    if(context->document->num_fields > 0) {
        //free the allocated memory in this document
        for(int i =0; i < context->document->num_fields; i++) {
            free(*(context->document->fields + i));
            free(*(context->document->field_names + i));
        }
        context->document->num_fields = 0;
    }
    for (;;) {
        //if(yajl_get_bytes_consumed(context->handle) == 0 || yajl_get_bytes_consumed(context->handle) >= context->buffer_size) {
        if(context->parse_state == 0) {
            context->bytes_consumed = 0;
            context->bytes_read = fread(context->buffer, 1, context->buffer_size, context->json_stream);
            if (context->bytes_read == 0) {
                //tyn_log(stderr, "Error reading json stream!");
                return NULL;
            }
        } else {
            context->parse_state = 0;
        }
            /* read file data, now pass to parser */
        yajl_status stat = yajl_parse(context->handle, context->buffer + context->bytes_consumed, context->bytes_read - context->bytes_consumed);
        
        if(stat == yajl_status_client_canceled) {
            context->bytes_consumed += yajl_get_bytes_consumed(context->handle);
            return reader->document;
        }
        if(stat == yajl_status_ok) {
            //need to read new block
        }

        
        if (stat != yajl_status_ok) {
            tyn_log(stderr,"%s\n", yajl_get_error(context->handle, 1, context->buffer + context->bytes_consumed, context->bytes_read - context->bytes_consumed));
            return NULL;
        }
    }

}

int tyn_reader_destroy(tyn_reader_t *reader) {
    tyn_document_t *document = reader->document;
    if(document->num_fields > 0) {
        //free the allocated memory in this document
        for(int i =0; i < document->num_fields; i++) {
            free(*(document->fields + i));
            free(*(document->field_names + i));
        }
        document->num_fields = 0;
    }
    free(document->fields);
    free(document->field_lengths);
    free(document->field_names);
    free(document->field_name_lengths);
    free(reader->document);
    tyn_reader_context_t *context = (tyn_reader_context_t *)reader->context;
    free(context->buffer);
    yajl_free(context->handle);
    free(context);
}

int tyn_reader_create(config_setting_t *params, tyn_reader_t *reader) {
    //get json pipe command
    char *pipe_command;
    if(!config_setting_lookup_string(params, "pipe_command", &pipe_command)) {
        tyn_log(stderr, "Json Pipe reader: 'pipe_command' not defined!");
        return TYN_ERROR;
    }
    
    //get buffer size
    int buffer_size;
    if(!config_setting_lookup_int(params, "buffer_size", &buffer_size)) {
        tyn_log(stderr, "Json Pipe reader: 'buffer_size' not defined!");
        return TYN_ERROR;
    }
    
    tyn_reader_context_t *context = (tyn_reader_context_t *)malloc( sizeof(tyn_reader_context_t) );
    yajl_handle handle = yajl_alloc(&callbacks, NULL, context);
    char *buffer = (char *) malloc( buffer_size );
    
    FILE *json_stream = popen("php export.php", "r");

    if(!json_stream) {
        tyn_log(stderr, "Failed to open json stream!");
        return TYN_ERROR;
    }

    context->buffer = buffer;
    context->buffer_size = buffer_size;
    context->json_stream = json_stream;
    context->handle = handle;
    context->depth = 0;
    context->state = 0;
    context->parse_state = 0;
    context->bytes_read = 0;
    context->bytes_consumed = 0;

    reader->context = context;
    tyn_document_t *document = (tyn_document_t *)malloc( sizeof(tyn_document_t) );
    //initialize the elements of document
    size_t max_num_fields = 256;
    document->fields = (char **)malloc( sizeof(char *) * max_num_fields );
    document->field_names = (char **)malloc( sizeof(char *) * max_num_fields );
    document->field_lengths = (size_t *)malloc( sizeof(size_t) * max_num_fields );
    document->field_name_lengths = (size_t *)malloc( sizeof(size_t) * max_num_fields );
    document->num_fields = 0;
    document->max_num_fields = max_num_fields;
    
    reader->document = document;

    return TYN_OK;
/*
    yajl_status stat = yajl_complete_parse(handle);
    if (stat != yajl_status_ok)
    {
        unsigned char * str = yajl_get_error(handle, 0, buffer, bytes_read);
        tyn_log(stderr,"%s", str);
        yajl_free_error(handle, str);
    }

    
    free(buffer);
 */   
}
