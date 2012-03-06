
/*
* Copyright (c) Z.G. Shi  <blueflycn at gmail dot com>
* Copyright (c) TYANSEARCH
* Make a better world~
*/

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <my_global.h>
#include <mysql.h>

#include "tyan.h"

typedef struct tyn_reader_context_t {
    MYSQL *conn;
    MYSQL_RES *res;
    char **field_names;
    size_t *field_name_lengths;
    size_t num_fields;
    size_t max_num_fields;
} tyn_reader_context_t;

/*
int tyn_reader_fetch_document(tyn_reader_t *reader, tyn_document_t *document) {
    tyn_reader_context_t *context = reader->context;
    MYSQL_ROW row = mysql_fetch_row( context->res );
    if(!row) {
        return TYN_DONE;
    } else {
        document->fields = row;
        document->field_lengths = mysql_fetch_lengths( context->res );
        document->field_names = context->field_names;
        document->num_fields = context->num_fields;
    }
    return TYN_OK; 
}
*/

tyn_document_t* tyn_reader_fetch_document(tyn_reader_t *reader) {
    tyn_reader_context_t *context = (tyn_reader_context_t *)reader->context;
    MYSQL_ROW row = mysql_fetch_row( context->res );
    if(!row) {
        return NULL;
    } else {
        reader->document->fields = row;
        reader->document->field_lengths = mysql_fetch_lengths( context->res );
        //reader->document->field_names = context->field_names;
        //reader->document->num_fields = context->num_fields;
    }
    return reader->document; 
}


int tyn_reader_destroy(tyn_reader_t *reader) {
    tyn_reader_context_t *context = (tyn_reader_context_t *)reader->context;
    free(context->field_names);
    free(context->field_name_lengths);
    mysql_free_result(context->res);
    mysql_close(context->conn);
    free(reader->document);
    free(reader->context);
}

int tyn_reader_create(config_setting_t *params, tyn_reader_t *reader) {
    /*
            host = "127.0.0.1";
            port = 3306;
            user = "root";
            password = "root";
            sql = "SELECT * FROM articles LIMIT 50000;";
            sql_pre = ["SET NAMES utf8"];
            sql_post = ["", "", ""];
    */

    //get source name defined in index
    char *host;
    if(!config_setting_lookup_string(params, "host", &host)) {
        tyn_log(stderr, "MySQL reader: 'host' not defined!");
        return TYN_ERROR;
    }

    //get port
    int port;
    if(!config_setting_lookup_int(params, "port", &port)) {
        tyn_log(stderr, "MySQL reader: 'port' not defined!");
        return TYN_ERROR;
    }
    
    //get user
    char *user;
    if(!config_setting_lookup_string(params, "user", &user)) {
        tyn_log(stderr, "MySQL reader: 'user' not defined!");
        return TYN_ERROR;
    }
    
    //get password
    char *password;
    if(!config_setting_lookup_string(params, "password", &password)) {
        tyn_log(stderr, "MySQL reader: 'password' not defined!");
        return TYN_ERROR;
    }

    //get database
    char *database;
    if(!config_setting_lookup_string(params, "database", &database)) {
        tyn_log(stderr, "MySQL reader: 'database' not defined!");
        return TYN_ERROR;
    }
    
    //get sql
    char *sql;
    if(!config_setting_lookup_string(params, "sql", &sql)) {
        tyn_log(stderr, "MySQL reader: 'sql' not defined!");
        return TYN_ERROR;
    }
    
    //connect to mysql
    //printf("MySQL client version: %s\n", mysql_get_client_info());
    MYSQL *conn;
    MYSQL_RES *res;
    //MYSQL_ROW row;
    MYSQL_FIELD *fields;
    conn = mysql_init(NULL);
    // Connect to database
    if(!mysql_real_connect(conn, host, user, password, database, port, NULL, 0)) {
        tyn_log(stderr, "%s", mysql_error(conn));
        return TYN_ERROR;
    } else {
        tyn_log(stdout, "connected to MySQL server %s:%d as %s, use database '%s'", host, port, user, database);
    }
    
    //get sql_pre
    config_setting_t *sql_pre = config_setting_get_member(params, "sql_pre");
    size_t num_sql_pre = config_setting_length(sql_pre);
    for(int i = 0; i < num_sql_pre; i++) {
        config_setting_t *sql = config_setting_get_elem(sql_pre, i);
        char *sql_str = config_setting_get_string(sql);
        tyn_log(stdout, "executing '%s'", sql_str);
        if(mysql_query(conn, sql_str)) {
            tyn_log(stderr, "%s", mysql_error(conn));
        }
    }
    
    tyn_log(stdout, "executing '%s'", sql);
    if (mysql_query(conn, sql)) {
        tyn_log(stderr, "%s", mysql_error(conn));
        return TYN_ERROR;
    }
    res = mysql_use_result(conn);
    fields = mysql_fetch_fields(res);
    
    tyn_reader_context_t* context = (tyn_reader_context_t *)malloc( sizeof(tyn_reader_context_t) );
    context->conn = conn;
    context->res = res;
    size_t num_fields = mysql_num_fields(res);
    char **field_names = (char **)malloc( sizeof(char *) * num_fields );
    size_t *field_name_lengths = (size_t *)malloc( sizeof(size_t) * num_fields );
    for(int i = 0; i < num_fields; i++ ) {
        *(field_names + i) = fields[i].name;
        printf("field name: %s\n", fields[i].name);
        *(field_name_lengths +i) = strlen(fields[i].name);
        //printf("field name length: %d\n", fields[i].length);
    }
    context->max_num_fields = num_fields;
    context->num_fields = num_fields;
    context->field_names = field_names;
    context->field_name_lengths = field_name_lengths;
    //initialize some of document elements once for all, since these won't be changed during a MySQL query
    /*
     * allocate memory for reader->document is required for every reader
     */
    reader->document = (tyn_document_t *)malloc( sizeof(tyn_document_t) );
    reader->document->field_names = context->field_names;
    reader->document->num_fields = context->num_fields;
    reader->document->field_name_lengths = context->field_name_lengths;
    
    reader->context = context;
    
    return TYN_OK;
 }
