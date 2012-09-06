/*
 * Copyright (C) 2012 Weichao Guo <weichao.guo@stanzax.org>
 * Copyright (C) 2012 Jinglei Ren <jinglei.ren@stanzax.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

//
//  cinquain_store_bdb.c
//  Cinquain-Store
//
//  Created by Weichao Guo <guoweichao2010@gmail.com> on 7/26/12.
//


#define _FILE_OFFSET_BITS 64
// const varibles
// in bytes

#define BIG_FILE_SIZE (1024*1024*1024)

#define NAME_LEN 16
#define DEFAULT_DB_HOME "./bdb"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "db.h"

#include "cinquain_store.h"
#include "internal.h"

// bdb context handler
static DB_ENV *db_env;
static DB *db;

//record the error number & error message
int errorNo = 0;

//big file root path
char bfileRootpath[256] = "./big_file/";
int rootpathLen = 0;

static int cinquainIncreaseBy(const char *key, const int key_length, int increment);
static char *cinquainKeyRef(const char *key, const int key_length);
static void cinquainReadAll();

static char *bfileReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length,
                         const offset_t file_size);
static int bfileWriteRange(const char *key, const int key_length,
                       offset_t offset,
                       const char *value, const offset_t value_length,
                       const offset_t file_size);
static offset_t bfileAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        const offset_t current_length,
                        const offset_t file_size);
static int bfileRemove(const char *key, const int key_length,
                   const offset_t file_size);
static char *bfilePath(const char *key, const int key_length); 


int cinquainInitBackStore(const int argc, const char *argv[]) {
    

    //get root path length
    rootpathLen = strlen(bfileRootpath);
    
    //create & open bdb's env & database
    //db name & filename
    char *db_name = malloc(sizeof(char)*NAME_LEN);
    char *db_filename = malloc(sizeof(char)*NAME_LEN);
    strcpy(db_name, argv[1]);
    strcpy(db_filename, db_name);
    strcat(db_filename, ".db");

    if (! (errorNo = db_env_create(&db_env, 0)) && 
        ! (errorNo = db_env->open(db_env, DEFAULT_DB_HOME, DB_CREATE | DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_TXN, 0)) && 
        ! (errorNo = db_create(&db, db_env, 0)) &&
        ! (errorNo = db->open(db, NULL, db_filename, db_name, DB_BTREE, DB_CREATE | DB_AUTO_COMMIT, 0))) {
        errorNo = 0;
    }
    else
        cinquainCloseBackStore();
    return errorNo;
}

int cinquainCloseBackStore() {
    //close db & db_env
    if (! (errorNo = db->close(db, 0)))
        return (errorNo = db_env->close(db_env, 0));
    else {
        db_env->close(db_env, 0);
        return errorNo;
    }
}

char *cinquainReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length,
                         const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        return bfileReadRange(key, key_length, offset, length, file_size);
    DBT db_key, db_data;
    char *buffer = NULL;
    memset(&db_key, 0, sizeof(DBT));
    memset(&db_data, 0, sizeof(DBT));
    db_key.data = (void *)key;
    db_key.ulen = db_key.size = key_length;
    db_data.flags = DB_DBT_MALLOC | DB_DBT_PARTIAL;
    db_data.dlen = length;
    db_data.doff = offset;
    if (! (errorNo = db->get(db, NULL, &db_key, &db_data, 0))) {
        //copy to buffer...
        //buffer = (char *)malloc(sizeof(char) * (db_data.size + 1));
        //memcpy(buffer, db_data.data, db_data.size);
        //buffer[db_data.size] = 0;
        buffer = (char *)db_data.data;
        buffer[length] = 0;
        return buffer;
    }
    return NULL;
}

int cinquainDeleteBufferHost(const char **value, const offset_t file_size) {

    free((char *)(*value));
    return 0;
}

int cinquainWriteRange(const char *key, const int key_length,
                       offset_t offset,
                       const char *value, const offset_t value_length,
                       const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        return bfileWriteRange(key, key_length, offset, value, value_length, file_size);
    DBT db_key, db_data;
    memset(&db_key, 0, sizeof(DBT));
    memset(&db_data, 0, sizeof(DBT));
    db_key.data = (void *)key;
    db_key.size = key_length;
    db_data.data = (void *)value;
    db_data.ulen = db_data.size = value_length;
    db_data.flags = DB_DBT_PARTIAL | DB_DBT_USERMEM;
    db_data.dlen = value_length;
    db_data.doff = offset;
    if (! (errorNo=db->put(db, NULL, &db_key, &db_data, 0)))
        return db_data.size;
    else
        return errorNo;
}

offset_t cinquainAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        const offset_t current_length,
                        const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        return bfileAppend(key, key_length, value, value_length, current_length, file_size);
    return cinquainWriteRange(key, key_length, current_length, value, value_length, file_size);
}

int cinquainIncrease(const char *key, const int key_length) {
    return cinquainIncreaseBy(key, key_length, 1);
}

int cinquainDecrease(const char *key, const int key_length) {
    return cinquainIncreaseBy(key, key_length, -1);
}

int cinquainIncreaseBy(const char *key, const int key_length, int increment) {

    DBT db_key, db_data;
    DB_TXN *txn;
    unsigned int ref = 0;
    memset(&db_key, 0, sizeof(DBT));
    memset(&db_data, 0, sizeof(DBT));
    //get the txn handle
    if ( errorNo = db_env->txn_begin(db_env, NULL, &txn, 0) )
        return errorNo;
    //record key reference with a suffix 'r'
    char *key_ref = cinquainKeyRef(key, key_length);
    db_key.data = (void *)key_ref;
    db_key.size = strlen(key_ref);
    db_data.data = (void *)&ref;
    db_data.size = db_data.ulen = sizeof(ref);
    db_data.flags = DB_DBT_USERMEM;
    if ((! (errorNo = db->get(db, txn, &db_key, &db_data, 0))) || (errorNo == DB_NOTFOUND)) {
        ref = (ref+increment>0 ? ref+increment : 0);
        if (! (errorNo = db->put(db, txn, &db_key, &db_data, 0))) {
            free(key_ref);
            if (! (errorNo = txn->commit(txn, 0)))
                return ref;
        }
        else
            txn->abort(txn);
    }
    else
        txn->abort(txn);
    return errorNo;
}

int cinquainRemove(const char *key, const int key_length,
                   const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        return bfileRemove(key, key_length, file_size);
    DBT db_key, db_keyr;
    DB_TXN *txn;
    memset(&db_key, 0, sizeof(DBT));
    memset(&db_keyr, 0, sizeof(DBT));
    db_key.data = (void *)key;
    db_key.size = key_length;
    //key reference number
    char *key_ref = cinquainKeyRef(key, key_length);
    db_keyr.data = (void *)key_ref;
    db_keyr.size = strlen(key_ref);
    //get the txn handle
    if ( errorNo = db_env->txn_begin(db_env, NULL, &txn, 0) )
        return errorNo;

    if ((! (errorNo = db->del(db, txn, &db_keyr, 0))) || (errorNo == DB_NOTFOUND)) {
        free(key_ref);
        if (! (errorNo = db->del(db, txn, &db_key, 0)))
            return (errorNo = txn->commit(txn, 0));
        else
            txn->abort(txn);
    }
    else
        txn->abort(txn);
    return errorNo;
}

int cinquainGetErr() {
    if (errorNo)
        printf("%d : %s\n", errorNo, db_strerror(errorNo));
    return errorNo;
}

static char *cinquainKeyRef(const char *key, const int key_length) {

    char suffix[5] = "_ref";
    char *key_ref = (char *)malloc(sizeof(char)*(key_length+4+1));
    memcpy(key_ref, key, key_length);
    strcpy(key_ref+key_length, suffix);
    return key_ref;
}
static void cinquainReadAll() {

    DBC *dbc;
    DBT key, data;
	/* Acquire a cursor for the database. */
	if ((errorNo = db->cursor(db, NULL, &dbc, 0)) != 0) {
		db->err(db, errorNo, "DB->cursor");
	}

	/* Initialize the key/data pair so the flags aren't set. */
	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	/* Walk through the database and print out the key/data pairs. */
	while ((errorNo = dbc->get(dbc, &key, &data, DB_NEXT)) == 0)
		printf("%.*s : %.*s\n",
		    (int)key.size, (char *)key.data,
		    (int)data.size, (char *)data.data);
	if (errorNo != DB_NOTFOUND) {
		db->err(db, errorNo, "DBcursor->get");
	}
    return;
}

static char *bfilePath(const char *key, const int key_length) {

    memcpy(bfileRootpath+rootpathLen, key, key_length);
    bfileRootpath[rootpathLen+key_length] = 0;
    return bfileRootpath;
}
static char *bfileReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length,
                         const offset_t file_size) {

    char *bfileBuffer = NULL;
    FILE *fp = fopen(bfilePath(key, key_length), "rb");
    if (fp) {
        if (!fseeko(fp, offset, SEEK_SET)) {
            bfileBuffer = calloc(sizeof(char), length+1);
            if (fread(bfileBuffer, sizeof(char), length, fp) != sizeof(char)*length) {
                free(bfileBuffer);
                bfileBuffer = NULL;
            }
        }
        fclose(fp);
    }
    return bfileBuffer;
}

static int bfileWriteRange(const char *key, const int key_length,
                       offset_t offset,
                       const char *value, const offset_t value_length,
                       const offset_t file_size) {

    int hasWritten = 0;
    FILE *fp = fopen(bfilePath(key, key_length), "wb");
    if (fp) {
        if (!fseeko(fp, offset, SEEK_SET)) {
            hasWritten = fwrite(value, sizeof(char), value_length, fp);
        }
        fclose(fp);
    }
    return hasWritten;
}
static offset_t bfileAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        const offset_t current_length,
                        const offset_t file_size) {

    offset_t hasWritten = 0;
    FILE *fp = fopen(bfilePath(key, key_length), "wb");
    if (fp) {
        if (!fseeko(fp, 0, SEEK_END)) {
            hasWritten = fwrite(value, sizeof(char), value_length, fp);
        }
        fclose(fp);
    }
    return hasWritten+current_length;
}
static int bfileRemove(const char *key, const int key_length,
                   const offset_t file_size) {

    return unlink(bfilePath(key, key_length));
}
