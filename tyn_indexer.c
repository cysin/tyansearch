
/*
* Copyright (c) Z.G. Shi  <blueflycn at gmail dot com>
* Make a better world~
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <dlfcn.h>
#include <time.h>
#include <stdint.h>

#include "qsort.h"
#include "tyn_indexer.h"

#define TYN_HASH_DOC_ID 1371954610u
#define TYN_HASH_FULLTEXT 3459865478u
#define TYN_HASH_ATTR_INT 3828627543u
#define TYN_HASH_ATTR_INT8 2738927051u
#define TYN_HASH_ATTR_INT16 4084979836u
#define TYN_HASH_ATTR_INT32 4047674610u
#define TYN_HASH_ATTR_INT64 1993453997u
#define TYN_HASH_ATTR_FLOAT 2228616072u
#define TYN_HASH_ATTR_TIMESTAMP 3869092598u


int tyn_compare_float(float *a, float *b) {
    //return (*a > *b) - (*a < *b);
    return (*a > *b) - (*a < *b);
}
int tyn_compare_uint32_t(uint32_t *a, uint32_t *b) {
    return (*a > *b) - (*a < *b);
}
tyn_config_t* load_config(char *config_file_name, config_t *cfg) {
    tyn_config_t *tyn_config = (tyn_config_t *)malloc( sizeof(tyn_config_t) );
    tyn_config->indexes_count = 0;
    tyn_config->indexes = NULL;

    tyn_config_indexer_t *tyn_config_indexer = (tyn_config_indexer_t *)malloc( sizeof(tyn_config_indexer_t) );
    tyn_config_searchd_t *tyn_config_searchd = (tyn_config_searchd_t *)malloc( sizeof(tyn_config_searchd_t) );

    //char *config_file_name = "tyan.conf";
    if(!config_read_file(cfg, config_file_name)) {
        tyn_log(stderr, "'%s' : can not open!", config_file_name);
        tyn_log(stderr, "line: %d\n", config_error_line (cfg));
        return TYN_ERROR;
    }
    
    //get indexer configurations    
    config_setting_t *indexer = config_lookup(cfg, "indexer");
    if(!indexer) {
        tyn_log(stderr, "'%s' : 'indexer' not defined!", config_file_name);
        return TYN_ERROR;
    }
    
    //get dynamic library directory
    if(!config_setting_lookup_string(indexer, "dl_directory", &tyn_config_indexer->dl_directory)) {
        tyn_log(stderr, "'%s' : 'dl_directory' not defined!", config_file_name);
        return TYN_ERROR;
    }
    
    //get memory limit
    if(!config_setting_lookup_int(indexer, "memory_limit", &tyn_config_indexer->memory_limit)) {
        tyn_log(stderr, "'%s' : 'memory' not defined!", config_file_name);
        return TYN_ERROR;
    }
    
    //get search daemon configurations    
    config_setting_t *searchd = config_lookup(cfg, "searchd");
    if(!searchd) {
        tyn_log(stderr, "'%s' : 'searchd' not defined!", config_file_name);
        return TYN_ERROR;
    }
    
    //get indexes configurations group
    config_setting_t *indexes = config_lookup(cfg, "indexes");
    if(!indexes) {
        tyn_log(stderr, "No indexes definition!");
        return TYN_ERROR;
    }

    //get sources configurations group
    config_setting_t *sources = config_lookup(cfg, "sources");
    if(!sources) {
        tyn_log(stderr, "No data sources definition!");
        return TYN_ERROR;
    }
    
    size_t indexes_count = config_setting_length(indexes);
    if(!indexes_count) {
        tyn_log(stderr, "Empty indexes!");
        return TYN_ERROR;
    }
    
    //initialize tyn_config_indexes
    tyn_config_index_t **tyn_config_indexes = (tyn_config_index_t **)malloc( indexes_count * sizeof(tyn_config_index_t *) );
    
    for(int i = 0; i < indexes_count; i++) {
        //get index configurations
        config_setting_t *index = config_setting_get_elem(indexes, i);
        char *index_name = config_setting_name(index);

        //get path
        char *path;
        if(!config_setting_lookup_string(index, "path", &path)) {
            tyn_log(stderr, "index '%s': 'path' not defined!", index_name);
            return TYN_ERROR;
        }
        
        //get source name defined in index
        char *source_name;
        if(!config_setting_lookup_string(index, "source", &source_name)) {
            tyn_log(stderr, "index '%s': 'source' not defined!", index_name);
            return TYN_ERROR;
        }

        //get source configurations
        config_setting_t *source = config_setting_get_member(sources, source_name);
        if(!source) {
            tyn_log(stderr, "%s definition not found!");
            return TYN_ERROR;
        }
        
        //get reader dynamic library path
        char *reader_path;
        if(!config_setting_lookup_string(source, "reader_path", &reader_path)) {
            tyn_log(stderr, "index '%s': '%s' : reader_path not defined!", index_name, source_name);
            return TYN_ERROR;
        }
        
        //get source dynamlic library configurations
        config_setting_t *reader_params = config_setting_get_member(source, "reader_params");
        if(!reader_params) {
            tyn_log(stderr, "index '%s': '%s' : reader_params not defined, ", index_name, source_name);
        }
        
        //get tokenizer dynamic library path
        char *tokenizer_path;
        if(!config_setting_lookup_string(index, "tokenizer_path", &tokenizer_path)) {
            tyn_log(stderr, "index '%s': 'tokenizer_path' not defined, use default tokenizer!", index_name);
            //return -1;
        }
        
        //get tokenizer dynamic library settings
        config_setting_t *tokenizer_params = config_setting_get_member(index, "tokenizer_params");
        if(!tokenizer_params) {
            tyn_log(stderr, "index '%s': 'tokenizer_params' not defined, use empty parameters", index_name);
        }
        
        config_setting_t *fields = config_setting_get_member(source, "fields");
        if(fields == NULL) {
            tyn_log(stderr, "index '%s': no fields group definition found", index_name);
            return TYN_ERROR;
        }
        if(config_setting_is_list(fields) == CONFIG_FALSE) {
            tyn_log(stderr, "index '%s': invald fields definition", index_name);
            return TYN_ERROR;
        }
        size_t num_fields = config_setting_length(fields);
        if(num_fields == 0) {
            tyn_log(stderr, "index '%s': no fields definition found", index_name);
            return TYN_ERROR;
        }
        size_t num_doc_id32_field = 0;
        size_t num_doc_id64_field = 0;
        size_t num_fulltext_fields = 0;
        size_t num_attr_int8_fields = 0;
        size_t num_attr_int16_fields = 0;
        size_t num_attr_int32_fields = 0;
        size_t num_attr_int64_fields = 0;
        size_t num_attr_float_fields = 0;
        size_t num_attr_timestamp_fields = 0;

        //validate fields
        for(int i = 0; i < num_fields; i++) {
            config_setting_t *field_settings = config_setting_get_elem(fields, i);
            if(config_setting_is_group(field_settings) == CONFIG_FALSE) {
                tyn_log(stderr, "index '%s': %dth field: invalid definition", index_name, i);
                return TYN_ERROR;
            }
            //size_t num_field_settings = config_setting_length(fields);
            if(config_setting_length(field_settings) < 2) {
                tyn_log(stderr, "index '%s': at least DOC_ID field and one other field must be defined", index_name);
                return TYN_ERROR;
            }
            char *field_name;
            char *field_type;
            int field_bits = 0 ;
            config_setting_lookup_string(field_settings, "name", &field_name);
            config_setting_lookup_string(field_settings, "type", &field_type);
            
            switch(tyn_hash32(field_type, strlen(field_type))) {
                case TYN_HASH_DOC_ID:
                    config_setting_lookup_int(field_settings, "bits", &field_bits);
                    switch(field_bits) {
                        case 0:
                        case 32:
                            num_doc_id32_field++;
                        break;
                        case 64:
                            num_doc_id64_field++;
                        break;
                        default:
                            tyn_log(stderr, "unallowed bits definition %d, use default 32bit", field_bits);
                            num_doc_id32_field++;
                        break;
                    }
                break;
                case TYN_HASH_FULLTEXT:
                    num_fulltext_fields++;
                break;
                case TYN_HASH_ATTR_INT:
                    config_setting_lookup_int(field_settings, "bits", &field_bits);
                    switch(field_bits) {
                        case 8:
                            num_attr_int8_fields++;
                        break;
                        case 16:
                            num_attr_int16_fields++;
                        break;
                        case 0:
                        case 32:
                            num_attr_int32_fields++;
                        break;
                        case 64:
                            num_attr_int64_fields++;
                        break;
                        default:
                            tyn_log(stderr, "unallowed bits definition: %d, use default 32 bit");
                            num_attr_int32_fields++;
                        break;
                    }
                break;
                case TYN_HASH_ATTR_FLOAT:
                    num_attr_float_fields++;
                break;
                case TYN_HASH_ATTR_TIMESTAMP:
                    num_attr_timestamp_fields++;
                break;
                default:
                    
                break;
            }
            if(!((num_doc_id32_field == 1 && num_doc_id64_field == 0) || (num_doc_id32_field == 0 && num_doc_id64_field == 1))) {
                tyn_log(stderr, "index '%s': invalid DOC_ID field definition, only one is allowed", index_name);
                return TYN_ERROR;
            }
        }
        
        char *doc_id_field;
        int doc_id_field_type = 0;
        char **fulltext_fields = (char **)malloc( num_fulltext_fields );
        //char **attr_int_fields = (char **)malloc( num_attr_int_fields );
        char **attr_int8_fields = (char **)malloc( num_attr_int8_fields );
        char **attr_int16_fields = (char **)malloc( num_attr_int16_fields );
        char **attr_int32_fields = (char **)malloc( num_attr_int32_fields );
        char **attr_int64_fields = (char **)malloc( num_attr_int64_fields );
        char **attr_float_fields = (char **)malloc( num_attr_float_fields );
        char **attr_timestamp_fields = (char **)malloc( num_attr_timestamp_fields );
        uint32_t *fulltext_field_hashes = (uint32_t *)malloc( num_fulltext_fields * sizeof(uint32_t) );
        //uint32_t *attr_int_field_hashes =(uint32_t *)malloc( num_attr_int_fields * sizeof(uint32_t) );
        uint32_t *attr_int8_field_hashes = (uint32_t *)malloc( num_attr_int8_fields * sizeof(uint32_t) );
        uint32_t *attr_int16_field_hashes = (uint32_t *)malloc( num_attr_int16_fields * sizeof(uint32_t) );
        uint32_t *attr_int32_field_hashes = (uint32_t *)malloc( num_attr_int32_fields * sizeof(uint32_t) );
        uint32_t *attr_int64_field_hashes = (uint32_t *)malloc( num_attr_int64_fields * sizeof(uint32_t) );
        uint32_t *attr_float_field_hashes = (uint32_t *)malloc( num_attr_float_fields * sizeof(uint32_t) );
        uint32_t *attr_timestamp_field_hashes = (uint32_t *)malloc( num_attr_timestamp_fields * sizeof(uint32_t) );

        num_fulltext_fields = 0;
        //um_attr_int_fields = 0;
        num_attr_int8_fields = 0;
        num_attr_int16_fields = 0;
        num_attr_int32_fields = 0;
        num_attr_int64_fields = 0;
        num_attr_float_fields = 0;
        num_attr_timestamp_fields = 0;
        for(int i = 0; i < num_fields; i++) {
            config_setting_t *field_settings = config_setting_get_elem(fields, i);
            char *field_name;
            char *field_type;
            int field_bits = 0;
            config_setting_lookup_string(field_settings, "name", &field_name);
            config_setting_lookup_string(field_settings, "type", &field_type);
            
            switch(tyn_hash32(field_type, strlen(field_type))) {
                case TYN_HASH_DOC_ID:
                    config_setting_lookup_int(field_settings, "bits", &field_bits);
                    switch(field_bits) {
                        case 64:
                            doc_id_field_type = 64;
                            num_doc_id64_field++;
                        break;
                        case 0:
                        case 32:
                        default:
                            doc_id_field_type = 32;
                            num_doc_id32_field++;
                        break;
                    }
                    doc_id_field = field_name;
                break;
                case TYN_HASH_FULLTEXT:
                    *(fulltext_fields + num_fulltext_fields) = field_name;
                    *(fulltext_field_hashes + num_fulltext_fields) = tyn_hash32(field_name, strlen(field_name));
                    num_fulltext_fields++;
                break;
                case TYN_HASH_ATTR_INT:
                    config_setting_lookup_int(field_settings, "bits", &field_bits);
                    switch(field_bits) {
                        case 8:
                            *(attr_int8_fields + num_attr_int8_fields) = field_name;
                            *(attr_int8_field_hashes + num_attr_int8_fields) = tyn_hash32(field_name, strlen(field_name));
                            num_attr_int8_fields++;
                        break;
                        case 16:
                            *(attr_int16_fields + num_attr_int16_fields) = field_name;
                            *(attr_int16_field_hashes + num_attr_int16_fields) = tyn_hash32(field_name, strlen(field_name));
                            num_attr_int16_fields++;
                        break;
                        case 64:
                            *(attr_int64_fields + num_attr_int64_fields) = field_name;
                            *(attr_int64_field_hashes + num_attr_int64_fields) = tyn_hash32(field_name, strlen(field_name));
                            num_attr_int64_fields++;
                        break;
                        case 0:
                        case 32:
                        default:
                            *(attr_int32_fields + num_attr_int32_fields) = field_name;
                            *(attr_int32_field_hashes + num_attr_int32_fields) = tyn_hash32(field_name, strlen(field_name));
                            num_attr_int32_fields++;
                        break;
                    }
                break;
                case TYN_HASH_ATTR_FLOAT:
                    *(attr_float_fields + num_attr_float_fields) = field_name;
                    *(attr_float_field_hashes + num_attr_float_fields) = tyn_hash32(field_name, strlen(field_name));
                    num_attr_float_fields++;
                break;
                case TYN_HASH_ATTR_TIMESTAMP:
                    *(attr_timestamp_fields + num_attr_timestamp_fields) = field_name;
                    *(attr_timestamp_field_hashes + num_attr_timestamp_fields) = tyn_hash32(field_name, strlen(field_name));
                    num_attr_timestamp_fields++;
                break;
                default:
                    
                break;
            }
        }
        
        //initialize the index config
        tyn_config_index_t *tyn_config_index = (tyn_config_index_t *)malloc( sizeof(tyn_config_index_t) );
        tyn_config_index->index_name = index_name;
        tyn_config_index->source_name = source_name;
        tyn_config_index->path = path;
        tyn_config_index->reader_path = reader_path;
        tyn_config_index->reader_params = reader_params;
        tyn_config_index->tokenizer_path = tokenizer_path;
        tyn_config_index->tokenizer_params = tokenizer_params;
        
        tyn_config_index->doc_id_field = doc_id_field;
        tyn_config_index->doc_id_field_type = doc_id_field_type;
        tyn_config_index->doc_id_field_hash = tyn_hash32(doc_id_field, strlen(doc_id_field));
        
        tyn_config_index->fulltext_fields = fulltext_fields;
        tyn_config_index->num_fulltext_fields = num_fulltext_fields;
        qsort(fulltext_field_hashes, num_fulltext_fields, sizeof(uint32_t), tyn_compare_uint32_t);
        tyn_config_index->fulltext_field_hashes = fulltext_field_hashes;
        
        tyn_config_index->attr_int8_fields = attr_int8_fields;
        tyn_config_index->num_attr_int8_fields = num_attr_int8_fields;
        qsort(attr_int8_field_hashes, num_attr_int8_fields, sizeof(uint32_t), tyn_compare_uint32_t);
        tyn_config_index->attr_int8_field_hashes = attr_int8_field_hashes;

        tyn_config_index->attr_int16_fields = attr_int16_fields;
        tyn_config_index->num_attr_int16_fields = num_attr_int16_fields;
        qsort(attr_int16_field_hashes, num_attr_int16_fields, sizeof(uint32_t), tyn_compare_uint32_t);
        tyn_config_index->attr_int16_field_hashes = attr_int16_field_hashes;

        tyn_config_index->attr_int32_fields = attr_int32_fields;
        tyn_config_index->num_attr_int32_fields = num_attr_int32_fields;
        qsort(attr_int32_field_hashes, num_attr_int32_fields, sizeof(uint32_t), tyn_compare_uint32_t);
        tyn_config_index->attr_int32_field_hashes = attr_int32_field_hashes;


        tyn_config_index->attr_int64_fields = attr_int64_fields;
        tyn_config_index->num_attr_int64_fields = num_attr_int64_fields;
        qsort(attr_int64_field_hashes, num_attr_int64_fields, sizeof(uint32_t), tyn_compare_uint32_t);
        tyn_config_index->attr_int64_field_hashes = attr_int64_field_hashes;

        tyn_config_index->attr_float_fields = attr_float_fields;
        tyn_config_index->num_attr_float_fields = num_attr_float_fields;
        qsort(attr_float_field_hashes, num_attr_float_fields, sizeof(uint32_t), tyn_compare_uint32_t);
        tyn_config_index->attr_float_field_hashes = attr_float_field_hashes;

        tyn_config_index->attr_timestamp_fields = attr_timestamp_fields;
        tyn_config_index->num_attr_timestamp_fields = num_attr_timestamp_fields;
        qsort(attr_timestamp_field_hashes, num_attr_timestamp_fields, sizeof(uint32_t), tyn_compare_uint32_t);
        tyn_config_index->attr_timestamp_field_hashes = attr_timestamp_field_hashes;

        tyn_config_index->num_fields = num_fields;
        *(tyn_config_indexes + i) = tyn_config_index;
    }

    memcpy(tyn_config->file_name, config_file_name, strlen(config_file_name));
    *(tyn_config->file_name + strlen(config_file_name)) = '\0';
    tyn_config->indexer = tyn_config_indexer;
    tyn_config->searchd = tyn_config_searchd;
    tyn_config->indexes_count = indexes_count;
    tyn_config->indexes = tyn_config_indexes;
  
  return tyn_config;

}

int tyn_get_array_idx_uint32_t(uint32_t element, uint32_t *array, size_t array_size) {
    //bsearch (const void *key, const void *array, size_t count, size_t size, comparison_fn_t compare)
    uint32_t *p = bsearch(&element, array, array_size, sizeof(uint32_t), tyn_compare_uint32_t);
    if(p != NULL) {
        return p - array;
    }
    return -1;
    /*
    printf("bsearch ret: %d\n", a);
    for(int i = 0; i < array_size; i++) {               
        if(*(array + i) == element) {                   
            return i;                                   
        }                                               
    }                                                   
    return -1;
    */
}

tyn_field_type_enum_t tyn_get_field_type(tyn_config_index_t *tyn_config_index, char* field_name, size_t field_name_length) {
    /*
     * IMPROVE: try some other algorithms to find mathing field type rahter than iterate int array, skiplist?
     */
    uint32_t field_hash = tyn_hash32(field_name, field_name_length);
    if(field_hash == tyn_config_index->doc_id_field_hash) {
        return TYN_FIELD_ID_T;
    }
    
    if(tyn_get_array_idx_uint32_t(field_hash, tyn_config_index->fulltext_field_hashes, tyn_config_index->num_fulltext_fields ) != -1) {
        return TYN_FIELD_FULLTEXT_T;
    }
    
    if(tyn_get_array_idx_uint32_t(field_hash, tyn_config_index->attr_int8_field_hashes, tyn_config_index->num_attr_int8_fields ) != -1) {
        return TYN_FIELD_ATTR_INT8_T;
    }

    if(tyn_get_array_idx_uint32_t(field_hash, tyn_config_index->attr_int16_field_hashes, tyn_config_index->num_attr_int16_fields ) != -1) {
        return TYN_FIELD_ATTR_INT16_T;
    }
    
    if(tyn_get_array_idx_uint32_t(field_hash, tyn_config_index->attr_int32_field_hashes, tyn_config_index->num_attr_int32_fields ) != -1) {
        return TYN_FIELD_ATTR_INT32_T;
    }
    
    if(tyn_get_array_idx_uint32_t(field_hash, tyn_config_index->attr_int64_field_hashes, tyn_config_index->num_attr_int64_fields ) != -1) {
        return TYN_FIELD_ATTR_INT64_T;
    }
    
    if(tyn_get_array_idx_uint32_t(field_hash, tyn_config_index->attr_float_field_hashes, tyn_config_index->num_attr_float_fields ) != -1) {
        return TYN_FIELD_ATTR_FLOAT_T;
    }

    if(tyn_get_array_idx_uint32_t(field_hash, tyn_config_index->attr_timestamp_field_hashes, tyn_config_index->num_attr_timestamp_fields ) != -1) {
        return TYN_FIELD_ATTR_TIMESTAMP_T;
    }

    return TYN_FIELD_UNDEFINED_T;
}

int tyn_build_index(tyn_config_t *tyn_config, char *index_name) {
    size_t memory_limit = tyn_config->indexer->memory_limit;
    printf("memory limit:%d\n", memory_limit);
    for(int i = 0; i < tyn_config->indexes_count; i++) {
        tyn_config_index_t *tyn_config_index = *(tyn_config->indexes + i);
        if(index_name) {
            if(strcmp(tyn_config_index->index_name, index_name) != 0) {
                continue;
            }
        }
        clock_t cl;
        cl = clock();
        
        //load source dynamic library
        void *dl_reader = dlopen(tyn_config_index->reader_path, RTLD_LAZY);
        void *dl_tokenizer = dlopen(tyn_config_index->tokenizer_path, RTLD_LAZY);
        
        if (!dl_reader) {
            tyn_log(stderr, "'%s' failed to load!", tyn_config_index->reader_path);
            tyn_log(stderr, dlerror());
        }
        if (!dl_tokenizer) {
            tyn_log(stderr, "'%s' failed to load!", tyn_config_index->tokenizer_path);
            tyn_log(stderr, dlerror());
        }

        //the function's prototype
        int (* tyn_reader_create)(config_setting_t *, tyn_reader_t *);
        int (* tyn_reader_destroy)(tyn_reader_t *);
        tyn_document_t* (* tyn_reader_fetch_document)(tyn_reader_t *);
        int (* tyn_tokenizer_create)(config_setting_t *, tyn_tokenizer_t *);
        int (* tyn_tokenizer_destroy)(tyn_tokenizer_t *);
        int (* tyn_tokenizer_set_text)(char *, size_t, tyn_tokenizer_t *);
        tyn_token_t * (* tyn_tokenizer_get_token)(tyn_tokenizer_t *);
        
        //initialize the reader create function
        tyn_reader_create = (int (*)(config_setting_t *, tyn_reader_t *)) dlsym(dl_reader, "tyn_reader_create");
        tyn_reader_destroy = (int (*)(tyn_reader_t *)) dlsym(dl_reader, "tyn_reader_destroy");
        tyn_reader_fetch_document = (int (*)(tyn_reader_t *)) dlsym(dl_reader, "tyn_reader_fetch_document");
        tyn_tokenizer_create = (int (*)(config_setting_t *, tyn_tokenizer_t *))dlsym(dl_tokenizer, "tyn_tokenizer_create");
        tyn_tokenizer_destroy = (int (*)(tyn_tokenizer_t *))dlsym(dl_tokenizer, "tyn_tokenizer_destroy");
        tyn_tokenizer_set_text = (int (*)(char *, size_t, tyn_tokenizer_t *))dlsym(dl_tokenizer, "tyn_tokenizer_set_text");
        tyn_tokenizer_get_token = (int (*)(tyn_tokenizer_t *))dlsym(dl_tokenizer, "tyn_tokenizer_get_token");
        
        if(tyn_reader_create == NULL || tyn_reader_destroy == NULL || tyn_reader_fetch_document == NULL) {
            tyn_log(stderr, "'%s': failed to load functions", tyn_config_index->reader_path);
            tyn_log(stderr, "%s", dlerror());
            return TYN_ERROR;
        }
        if(tyn_tokenizer_create == NULL || tyn_tokenizer_destroy == NULL || tyn_tokenizer_set_text == NULL || tyn_tokenizer_get_token == NULL) {
            tyn_log(stderr, "'%s': failed to load functions", tyn_config_index->tokenizer_path);
            tyn_log(stderr, "%s", dlerror());
            return TYN_ERROR;
        }

        tyn_reader_t *reader = (tyn_reader_t *)malloc( sizeof(tyn_reader_t) );
        (*tyn_reader_create)(tyn_config_index->reader_params, reader);
        
        tyn_tokenizer_t *tokenizer = (tyn_tokenizer_t *) malloc( sizeof(tyn_tokenizer_t) );
        (*tyn_tokenizer_create)(tyn_config_index->tokenizer_params, tokenizer);
        
        tyn_document_t *document;
        size_t bytes_collected = 0;
        
        //prepare the memory buffers
        
        
        while( (document = (*tyn_reader_fetch_document)(reader)) != NULL) {
            uint32_t document_id = 0;
            //first we get document id
            char id_string[1024];//fix this if length > 1024?
            for(int i = 0; i < document->num_fields; i++) {
                tyn_field_type_enum_t field_type = tyn_get_field_type(tyn_config_index, *(document->field_names + i), *(document->field_name_lengths + i));
                if(field_type == TYN_FIELD_ID_T) {
                    memcpy(id_string, *(document->fields + i), *(document->field_lengths + i));
                    *(id_string + *(document->field_lengths + i)) = '\0';
                    document_id = atoi(id_string);
                    //printf("%s\n", id_string);
                }
            }
            if(document_id == 0) {
                tyn_log(stderr, "document id not found, contiue\n");
                continue;
            }

            printf("document id: %u\r", document_id);
            fflush(stdout);
            for(int i = 0; i < document->num_fields; i++) {
                tyn_field_type_enum_t field_type = tyn_get_field_type(tyn_config_index, *(document->field_names + i), *(document->field_name_lengths + i));
                switch(field_type) {
                    case TYN_FIELD_ID_T:
                        printf("id: ");
                    break;
                    case TYN_FIELD_FULLTEXT_T:
                        (*tyn_tokenizer_set_text)(document->fields[i], document->field_lengths[i], tokenizer);
                        tyn_token_t *token;
                        while((token = (*tyn_tokenizer_get_token)(tokenizer)) != NULL) {
                            //printf("%d ", token->pos);
                            //fwrite(token->str, 1, token->str_len, stdout);
                    
                            //printf(" _ ");
                        }
                        printf("fulltext: ");
                    break;
                    case TYN_FIELD_ATTR_INT8_T:
                        printf("int8: ");
                    break;
                    case TYN_FIELD_ATTR_INT16_T:
                        printf("int16: ");
                    break;
                    case TYN_FIELD_ATTR_INT32_T:
                        printf("int32: ");
                    break;
                    case TYN_FIELD_ATTR_INT64_T:
                        printf("int64: ");
                    break;
                    case TYN_FIELD_ATTR_FLOAT_T:
                        printf("float: ");
                    break;
                    case TYN_FIELD_ATTR_TIMESTAMP_T:
                        printf("timestamp: ");
                    break;
                    case TYN_FIELD_ATTR_MVA_T:
                        /*
                         * currently it's better to save MVA attributes as a 'delimiter' delimiterred string, then we split it into sub-strings
                         */
                    break;
                    default:
                        printf("undefined field~: ");
                    break;
                }
                fwrite(document->field_names[i], 1, document->field_name_lengths[i], stdout);
                printf("\n");
                /*
                fwrite(document->field_names[i], 1, document->field_name_lengths[i], stdout);
                printf(":");
                fwrite(document->fields[i], 1, document->field_lengths[i], stdout);
                printf("\n");
                */
                

                bytes_collected += document->field_lengths[i];
                
                //printf("\n");
                //printf("%s:%s\n", document->field_names[i], document->fields[i]);
            }
        }
        printf("bytes_collected: %zu\n", bytes_collected);
        (*tyn_reader_destroy)(reader);
        (*tyn_tokenizer_destroy)(tokenizer);
        dlclose(dl_reader);
        dlclose(dl_tokenizer);
        free(document);
        free(reader);
        free(tokenizer);
        printf("index name: %s\n", tyn_config_index->index_name);
        printf("path: %s\n", tyn_config_index->path);
        printf("source name: %s\n", tyn_config_index->source_name);
        printf("reader path: %s\n", tyn_config_index->reader_path);
        printf("tokenizer path: %s\n", tyn_config_index->tokenizer_path);
        printf("id field name: %s\n", tyn_config_index->doc_id_field);
        cl = clock() - cl;
        float seconds = 1.0 * cl / CLOCKS_PER_SEC;
        printf("time took: %.3f sec\n", seconds);
        printf("%d bytes/s\n", bytes_collected / (int)seconds);
       
    }
    
    
    printf("tyn_config->indexer->dl_directory: %s\n", tyn_config->indexer->dl_directory);
    printf("tyn_config->indexer->memory_limit: %d\n", tyn_config->indexer->memory_limit);   
}


int main(int argc, char **argv) {
    config_t cfg;
    config_init(&cfg);
    char *config_file_name = "tyan.conf";
    tyn_config_t *tyn_config = load_config(config_file_name, &cfg);
    if(tyn_config == TYN_ERROR) {
        tyn_log(stderr, "Failed parsing %s", config_file_name);
        return TYN_ERROR;
    }
    tyn_build_index(tyn_config, "mysql_index");
    
 
    config_destroy(&cfg);
}
