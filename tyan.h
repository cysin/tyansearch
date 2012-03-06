
/*
* Copyright (c) Z.G. Shi  <blueflycn at gmail dot com>
* Make a better world~
*/

#ifndef _TYAN_H_
#define _TYAN_H__

#define  TYN_OK          0
#define  TYN_ERROR      -1
#define  TYN_AGAIN      -2
#define  TYN_BUSY       -3
#define  TYN_DONE       -4
#define  TYN_DECLINED   -5
#define  TYN_ABORT      -6

#include "tyn_utils.h"
#include "libconfig/libconfig.h"


typedef struct tyn_config_indexer_t {
    char *dl_directory;
    size_t memory_limit;
} tyn_config_indexer_t;

typedef struct tyn_config_searchd_t {
    char *host;
    int port;
    int max_match;
} tyn_config_searchd_t;

typedef struct tyn_config_index_t {
    char *index_name;
    char *path;
    
    char *source_name;
    char *reader_path;
    config_setting_t *reader_params;
    
    char *tokenizer_path;
    config_setting_t *tokenizer_params;
    
    char* doc_id_field;
    int doc_id_field_type;
    unsigned int doc_id_field_hash;
    
    char **fulltext_fields;
    size_t num_fulltext_fields;
    unsigned int *fulltext_field_hashes;
    
    char **attr_int8_fields;
    size_t num_attr_int8_fields;
    unsigned int *attr_int8_field_hashes;
    
    char **attr_int16_fields;
    size_t num_attr_int16_fields;
    unsigned int *attr_int16_field_hashes;
    
    char **attr_int32_fields;
    size_t num_attr_int32_fields;
    unsigned int *attr_int32_field_hashes;
    
    char **attr_int64_fields;
    size_t num_attr_int64_fields;
    unsigned int *attr_int64_field_hashes;
    
    char **attr_float_fields;
    size_t num_attr_float_fields;
    unsigned int *attr_float_field_hashes;
    
    char **attr_timestamp_fields;
    size_t num_attr_timestamp_fields;
    unsigned int *attr_timestamp_field_hashes;
    
    size_t num_fields;
} tyn_config_index_t;

typedef struct tyn_config_t {
    char file_name[128];
    tyn_config_indexer_t *indexer;
    tyn_config_searchd_t *searchd;
    size_t indexes_count;
    tyn_config_index_t **indexes;
} tyn_config_t;

typedef struct tyn_document_t {
    char **fields;
    char **field_names;
    size_t *field_lengths;
    size_t *field_name_lengths;
    size_t num_fields;
    size_t max_num_fields;
} tyn_document_t;

typedef enum tyn_field_type_enum_t {
    TYN_FIELD_ID_T,
    TYN_FIELD_FULLTEXT_T,
    TYN_FIELD_ATTR_INT8_T,
    TYN_FIELD_ATTR_INT16_T,
    TYN_FIELD_ATTR_INT32_T,
    TYN_FIELD_ATTR_INT64_T,
    TYN_FIELD_ATTR_FLOAT_T,
    TYN_FIELD_ATTR_STRING_T,
    TYN_FIELD_ATTR_TIMESTAMP_T,
    TYN_FIELD_ATTR_MVA_T,
    TYN_FIELD_UNDEFINED_T
} tyn_field_type_enum_t;

/*
typedef struct tyn_field_type_t {
    int idx;
    tyn_field_type_enum_t type;
} tyn_field_type_t;
*/

typedef struct tyn_reader_t {
    tyn_document_t *document;
    void *context;
} tyn_reader_t;

typedef struct tyn_token_t{
    char *str;
    size_t str_len;
    size_t pos;
} tyn_token_t;

typedef struct tyn_tokenizer_t {
    tyn_token_t *token;
    void *context;
} tyn_tokenizer_t;
#endif /* _TYAN_H_ */
