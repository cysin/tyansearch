
/*
* Copyright (c) Z.G. Shi  <blueflycn at gmail dot com>
* Copyright (c) TYANSEARCH
* Make a better world~
*/

#include "darts.h"
extern "C" {
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <math.h>
#include <ctype.h>

#include "tyan.h"

int tyn_tokenizer_create(config_setting_t *, tyn_tokenizer_t *);
int tyn_tokenizer_destroy(tyn_tokenizer_t *);
int tyn_tokenizer_set_text(char *, size_t, tyn_tokenizer_t *);
tyn_token_t* tyn_tokenizer_get_token(tyn_tokenizer_t *);
}

unsigned char utf8_length_table[] = {
    /*
    * 1-4:  leading byte of UTF-8 sequence, and its value is of sequence's length
    * 8:    content byte(not a leading byte)
    * 16:   illegal byte, it never apperas in a UTF-8 sequence
    */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //0-15
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //16-31
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //32-47
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //48-63
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //64-79
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //80-95
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //96-111
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //112-127
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    16, 16, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, //192-207
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, //208-223
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, //224-239
    4, 4, 4, 4, 4, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16  //240-255
};

typedef struct tyn_tokenizer_context_t {
    Darts::DoubleArray *darray;
    char *text;
    char *text_max;
    size_t text_length;
    char *text_cur;
    size_t pos;
    int mmap_fd;
    size_t mmap_file_size;
    void *mmap_file;
} tyn_tokenizer_context_t;


size_t get_token_length(char *text, size_t text_length, Darts::DoubleArray *darray) {
    Darts::DoubleArray::result_pair_type  result_pair_a[64];
    Darts::DoubleArray::result_pair_type  result_pair_b[64];
    Darts::DoubleArray::result_pair_type  result_pair_c[64];

    size_t num_a, num_b, num_c; //A B C stand for the first, sencod, third word in a chunk
    register size_t cur_a, cur_b, cur_c;
    char *text_a, *text_b, *text_c;
    size_t text_length_a, text_length_b, text_length_c;
    
    //we use a float 10-element-array to store a chunk
    //0-word count
    //1-3 byte length of each word
    //4-6 frequence of each word
    //7-9 character count of each word

    float chunks[64][10]={{0}};
    size_t chunk_cur=0;
    size_t chunk_count;

    float chunk_a[10]={0};
    float chunk_b[10]={0};

    size_t byte_count;
    register size_t byte_cur;
    size_t chunk_size=sizeof(float)*10;        

    size_t clip[64];//temporary cursor data when filtering
    size_t best[64];//the best result. There may be more than 1 chunk at last, so we use array
    register size_t clip_cur=0;
    size_t clip_size=0;
    register size_t best_cur=0;
    size_t result_size = sizeof(int)*64;

    float lengths[64]={0}; //chunks' lengths
    float word_counts[64]={0}; //for chunks' average lengths, but we use word count when filtering with this rule
    float variances[64]={0}; //chunks' 
    float sdmfs[64]={0}; //chunks' sum of degree of morphemic freedom of one-character words

    float max_chunk_length=0;
    float min_word_count;//used to filtering with max average length, which also means having the minimal word count;
    float min_variance;
    float avg_length;
    int int_temp; //mostly for filtering with min variance
    float float_temp; ////mostly for filtering with min variance
    float max_sdmf=0;

    text_a=text;
    text_length_a=text_length;
    //get all the chunks
    num_a = darray->commonPrefixSearch(text_a, result_pair_a, 64, text_length_a);
    if(num_a == 0) {
        size_t str_len = utf8_length_table[(unsigned char)*text_a];
        if(str_len == 1 && isalnum(*text_a) ) {
            while(darray->commonPrefixSearch(text_a + str_len, result_pair_a, 64, text_length_a - str_len) == 0 && utf8_length_table[(unsigned char)*(text_a + str_len)] == 1 && 
                ( (isalpha(*text_a) && isalpha(*(text_a + str_len))) || (isdigit(*text_a) && isdigit(*(text_a + str_len))) ) && str_len < text_length_a ) {
                str_len++;
            }
        }
        return str_len;
    }
    
    for(cur_a=0;cur_a<num_a;cur_a++) {
        chunk_a[0] = 1;
        chunk_a[1] = result_pair_a[cur_a].length;
        chunk_a[4] = result_pair_a[cur_a].value;
        //character count
        byte_count=0;
        for(byte_cur=0;byte_cur<chunk_a[1];byte_cur++) {
            if(utf8_length_table[(unsigned char)(*(text_a+byte_cur))]<0x08) {
                byte_count++;
            }
        }
        chunk_a[7] = byte_count;

        text_b = text_a + result_pair_a[cur_a].length;
        text_length_b = text_length_a - result_pair_a[cur_a].length;

        num_b = darray->commonPrefixSearch(text_b, result_pair_b, 64, text_length_b);
        if(num_b) {        
            for(cur_b=0;cur_b<num_b;cur_b++) {
                memcpy(chunk_b, chunk_a, chunk_size);//initialize chunk B
                chunk_b[0] = 2;            
                chunk_b[2] = result_pair_b[cur_b].length;
                chunk_b[5] = result_pair_b[cur_b].value;
                //character count
                byte_count=0;
                for(byte_cur=0;byte_cur<chunk_b[2];byte_cur++) {
                    if(utf8_length_table[(unsigned char)(*(text_b+byte_cur))]<0x08) {
                        byte_count++;
                    }
                }
                chunk_b[8] = byte_count;

                text_c = text_b + result_pair_b[cur_b].length;
                text_length_c = text_length_b - result_pair_b[cur_b].length;
                num_c = darray->commonPrefixSearch(text_c , result_pair_c, 64, text_length_c);
                if(num_c) {
                    for(cur_c=0;cur_c<num_c;cur_c++) {
                        memcpy(chunks[chunk_cur], chunk_b, chunk_size);

                        chunks[chunk_cur][0] = 3;
                        chunks[chunk_cur][3] = result_pair_c[cur_c].length;
                        chunks[chunk_cur][6] = result_pair_c[cur_c].value;
                        //character count
                        byte_count=0;
                        for(byte_cur=0;byte_cur<chunks[chunk_cur][3];byte_cur++) {
                            if(utf8_length_table[(unsigned char)(*(text_c+byte_cur))]<0x08) {
                                byte_count++;
                            }
                        }
                        chunks[chunk_cur][9] = byte_count;

                        chunk_cur++;
                    }
                } else {
                    memcpy(chunks[chunk_cur], chunk_b, chunk_size);
                    chunk_cur++;
                }//if(num_c)
            }
        } else {
            memcpy(chunks[chunk_cur], chunk_a, chunk_size);
            chunk_cur++;
        }//if(num_b)
    }//the main loop of obtaining chunks

    chunk_count=chunk_cur;//just remember the chunks count

    //filter with max length;
    for(chunk_cur=0;chunk_cur<chunk_count;chunk_cur++) {
        lengths[chunk_cur] = chunks[chunk_cur][7] + chunks[chunk_cur][8] + chunks[chunk_cur][9];
        max_chunk_length = max_chunk_length > lengths[chunk_cur] ? max_chunk_length : lengths[chunk_cur];
    }
    for(chunk_cur=0;chunk_cur<chunk_count;chunk_cur++) {
        if(max_chunk_length == lengths[chunk_cur]) {
            best[best_cur++]=chunk_cur;
        }
    }
    if(best_cur<2) {
        return (size_t)chunks[best[0]][1];
    }

    //filter with average word legnth

    memcpy(clip, best, result_size);
    clip_size=best_cur;
    //initialize the first one
    word_counts[clip[0]] = chunks[clip[0]][0];
    min_word_count=chunks[clip[0]][0];

    for(clip_cur=1;clip_cur<clip_size;clip_cur++) {//notice that clip_cur starts from 1 (not 0)
        word_counts[clip[clip_cur]] = chunks[clip[clip_cur]][0];
        min_word_count = min_word_count < word_counts[clip[clip_cur]] ? min_word_count : word_counts[clip[clip_cur]];
    }
    best_cur=0;
    for(clip_cur=0;clip_cur<clip_size;clip_cur++) {
        if(min_word_count == word_counts[clip[clip_cur]]) {
            best[best_cur++]=clip[clip_cur];
        }
    }
    if(best_cur<2) {
        return (size_t)chunks[best[0]][1];
    }
    //filter with min variance
    memcpy(clip, best, result_size);
    clip_size=best_cur;

    //initialize the first one
    avg_length = (chunks[clip[0]][7]+chunks[clip[0]][8]+chunks[clip[0]][9])/chunks[clip[0]][0];
    for(int_temp=0;int_temp<chunks[clip[0]][0];int_temp++) {
        float_temp= avg_length - chunks[clip[0]][int_temp+7];
        variances[clip[0]] += float_temp*float_temp;
    }
    variances[clip[0]] = sqrt(variances[clip[0]]/chunks[clip[0]][0]);
    min_variance = variances[clip[0]];

    for(clip_cur=1;clip_cur<clip_size;clip_cur++) {//notice that clip_cur starts from 1, not 0
        avg_length = (chunks[clip[clip_cur]][7]+chunks[clip[clip_cur]][8]+chunks[clip[clip_cur]][9])/chunks[clip[clip_cur]][0];
        for(int_temp=0;int_temp<chunks[clip[clip_cur]][0];int_temp++) {
            float_temp = avg_length - chunks[clip[clip_cur]][int_temp+7];
            variances[clip[clip_cur]] += float_temp*float_temp;
        }
        variances[clip[clip_cur]] = sqrt(variances[clip[clip_cur]]/chunks[clip[clip_cur]][0]);
        min_variance = min_variance < variances[clip[clip_cur]] ? min_variance : variances[clip[clip_cur]];
    }
    best_cur=0;
    for(clip_cur=0;clip_cur<clip_size;clip_cur++) {
        if(min_variance == variances[clip[clip_cur]]) {
            best[best_cur++]=clip[clip_cur];
        }
    }
    if(best_cur<2) {
        return (size_t)chunks[best[0]][1];
    }
    //filter with max SDMF(sum of degree of morphemic freedom of one-character words)
    memcpy(clip, best, result_size);
    clip_size=best_cur;

    for(clip_cur=0;clip_cur<clip_size;clip_cur++) {
        for(int_temp=0;int_temp<chunks[clip[clip_cur]][0];int_temp++) {
            if(chunks[clip[clip_cur]][int_temp+7]==1) {
                sdmfs[clip[clip_cur]]+=log(chunks[clip[clip_cur]][int_temp+4]);
            }
        }
        max_sdmf = max_sdmf > sdmfs[clip[clip_cur]] ? max_sdmf : sdmfs[clip[clip_cur]];
    }
    best_cur=0;
    for(clip_cur=0;clip_cur<clip_size;clip_cur++) {
        if(max_sdmf == sdmfs[clip[clip_cur]]) {
            best[best_cur++]=clip[clip_cur];
        }
    }
    //if(best_cur<2) {
    return (size_t)chunks[best[0]][1];
    //}    
}

int is_valid_token(char *str, size_t str_len) {
    if(str_len == 1 && !isalnum(*str)) {
        return 0;
    }
    return 1;
}

tyn_token_t* tyn_tokenizer_get_token(tyn_tokenizer_t *tokenizer) {
    tyn_tokenizer_context_t *context = (tyn_tokenizer_context_t *)tokenizer->context;
    size_t token_length;
    while(1) {
        if(context->text_max == context->text_cur) {
            return NULL;
        }
        token_length = get_token_length(context->text_cur, context->text_max - context->text_cur, context->darray);
        if(!is_valid_token(context->text_cur, token_length)) {
            context->pos++;
            context->text_cur += token_length;
        } else {
            break;
        }

    }
    tokenizer->token->str = context->text_cur;
    tokenizer->token->str_len = token_length;
    tokenizer->token->pos = context->pos;
    context->pos++;
    context->text_cur += token_length;
    return tokenizer->token; 
}

int tyn_tokenizer_set_text(char *text, size_t text_length, tyn_tokenizer_t *tokenizer) {
    tyn_tokenizer_context_t *context = (tyn_tokenizer_context_t *)tokenizer->context;
    context->text = text;
    context->text_max = text + text_length;
    context->text_length = text_length;
    context->text_cur = context->text;
    context->pos = 0;
    return TYN_OK;
}

int tyn_tokenizer_destroy(tyn_tokenizer_t *tokenizer) {
    tyn_tokenizer_context_t *context = (tyn_tokenizer_context_t *)tokenizer->context;
    delete context->darray;
    munmap(context->mmap_file, context->mmap_file_size);
    close(context->mmap_fd);
    free(tokenizer->token);
    free(tokenizer->context);
    return TYN_OK;
}

int tyn_tokenizer_create(config_setting_t *params, tyn_tokenizer_t *tokenizer) {
    //get dictionary
    const char *dictionary_path;
    if(!config_setting_lookup_string(params, "dictionary_path", &dictionary_path)) {
        tyn_log(stderr, "MMSEG Tokenizer: 'dictionary_path' not defined!");
        return TYN_ERROR;
    }

    //get stopwords dictionary
    const char *stopwrods_dictionary_path;
    if(!config_setting_lookup_string(params, "stopwrods_dictionary_path", &stopwrods_dictionary_path)) {
        tyn_log(stderr, "MMSEG Tokenizer: 'stopwrods_dictionary_path' not defined!");
        //return TYN_ERROR;
    }
    
    struct stat sb;
    if(stat(dictionary_path, &sb) == -1) {
        tyn_log(stderr, "Tokenizer MMSEG: Can not analyze '%s'", dictionary_path);
        return TYN_ERROR;
    }
    
    int mmap_fd;
    if((mmap_fd = open(dictionary_path, O_RDONLY)) == -1) {
        tyn_log(stderr, "Tokenizer MMSEG: Can not open '%s'",dictionary_path);
        return TYN_ERROR;
    }
    void *mmap_file;
    if(!(mmap_file = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, mmap_fd, 0))) {
        tyn_log(stderr, "Can not map '%s' to memory!", dictionary_path);
        return TYN_ERROR;
    }

    Darts::DoubleArray *darray = new Darts::DoubleArray;
    darray->set_array(mmap_file);
    tyn_tokenizer_context_t* context = (tyn_tokenizer_context_t *)malloc( sizeof(tyn_tokenizer_context_t) );
    
    context->mmap_fd = mmap_fd;
    context->mmap_file = mmap_file;
    context->mmap_file_size = sb.st_size;
    context->darray = darray;
    context->pos = 0;
    tokenizer->context = context;
    tokenizer->token = (tyn_token_t *) malloc( sizeof(tyn_token_t));
    
    return TYN_OK;
 }
