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
//  cinquain_store_redis.c
//  Cinquain-Store
//
//  Created by Jinglei Ren <jinglei.ren@gmail.com> on 2/16/12.
//  Modified by Weichao Guo <guoweichao2010@gmail.com>.
//


#define _FILE_OFFSET_BITS 64
// const varibles
// in bytes
//#define BLOCK_SIZE (512*1024*1024) 
#define BLOCK_SIZE (4*1024*1024) 
#define BIG_FILE_SIZE (16*1024*1024)
// for cinquain redis command...
#define ARGV_ARRAY_LEN 4
#define KEY_LEN 32
#define BUF_LEN 16

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "cinquain_store.h"
#include "internal.h"
#include "hiredis/hiredis.h"

// redis context handler
static redisContext *c[256];

// redis server config
static char *redisIp[256];
static int redisPort[256];
static int redisServerNum = 0;

// refks is a key suffix to record reference & numks is a key suffix to record block number
static unsigned int numks = 0xffffffff;
static unsigned int  refks = 0x00000000;

// key extend part length ...
static unsigned int keyExLength = sizeof(unsigned int);

static struct timeval timeout = { 1, 500000 }; // 1.5 seconds

//record the error number & error message
int errorNo = 0;
char errorMsg[64];

//big file root path
char bfileRootpath[256] = "./big_file/";
int rootpathLen = 0;

//big file read buffer point
char *bfileBuffer;

static redisContext *cinquainGetContext(const char *key, const int key_length);
static workBlocks *cinquainSetWorkBlocks(workBlocks *wbs, offset_t offset, offset_t length, const char *value);
static redisReply *cinquainReadBlock(const char *key, const int key_length, workBlock *wb);
static int cinquainWriteBlock(const char *key, const int key_length, workBlock *wb);
static int cinquainAppendBlock(const char *key, const int key_length, workBlock *wb);
static int cinquainDeleteBlock(const char *key, const int key_length, workBlock *wb);
static int cinquainStrlenBlock(const char *key, const int key_length, workBlock *wb);
static int cinquainIncreaseBy(const char *key, const int key_length, int increment);
static int cinquainErrLog(redisContext *c, redisReply *r);

static char **bfileReadRange(const char *key, const int key_length,
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

static redisReply *cinquainRedisCommand(redisContext *c, char *cmd, const char *key, const int key_length, const char *key_ex, const int key_ex_length, offset_t start, offset_t end, char *value, offset_t value_length);


int cinquainInitBackStore(const int argc, const char *argv[]) {
    
    char ip[64];
    int port, i=0;

    //get root path length
    rootpathLen = strlen(bfileRootpath);
    //read redis server config & get connections
    FILE *fp = fopen(argv[1], "r");
    if (fp != NULL) {
        while(!feof(fp)) {
            fscanf(fp, "%s %d\n", ip, &port);
            if (port < 0)
                c[i] = redisConnectUnix(ip);
                //c[i] = redisConnectUnixWithTimeout(ip, timeout);
            else
                c[i] = redisConnect(ip, port);
                //c[i] = redisConnectWithTimeout(ip, port, timeout);
            if (!c[i]->err) {
                redisIp[i] = malloc(sizeof(char) * (strlen(ip) + 1 ));
                strcpy(redisIp[i], ip);
                redisPort[i++] = port;
            }
            else {
                errorNo = CINQUAIN_ERR_CONNECTION;
                break;
            }
        }
        fclose(fp);
        redisServerNum = i;
    }
    else
        errorNo = CINQUAIN_ERR_CONFIG;
    return !errorNo ? redisServerNum : errorNo;
}

char **cinquainReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length,
                         const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        return bfileReadRange(key, key_length, offset, length, file_size);
    redisReply *r = NULL;
    workBlocks wbs = {NULL, 0};
    cinquainSetWorkBlocks(&wbs, offset, length, NULL);
    if (!wbs.wb)
        return NULL;
    while (wbs.blocks--) {
        r = cinquainReadBlock(key, key_length, &wbs.wb[wbs.blocks]);
        if (r && wbs.blocks)
            freeReplyObject(r);
        else
            break;
    }
    if (r && wbs.wb[0].buffer) {
        free(r->str);
        r->str = wbs.wb[0].buffer;
    }
    if (!r && wbs.wb[0].buffer) 
        free(wbs.wb[0].buffer);
    free(wbs.wb);
    return r ? &r->str : NULL;
}

int cinquainDeleteBufferHost(const char **value, const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        free((char *)(*value));
    else
        freeReplyObject(cinquainBufferHost(value, redisReply, str));
    return 0;
}

int cinquainWriteRange(const char *key, const int key_length,
                       offset_t offset,
                       const char *value, const offset_t value_length,
                       const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        return bfileWriteRange(key, key_length, offset, value, value_length, file_size);
    int hasWritten = 0, tErrorNo;
    workBlocks wbs = {NULL, 0};
    cinquainSetWorkBlocks(&wbs, offset, value_length, value);
    unsigned int blockNum = wbs.wb[wbs.blocks-1].id; 
    workBlock wb = {numks, 0, keyExLength, NULL};
    redisReply *r = NULL;
    unsigned int oldBlockNum = 0;
    
    if (wbs.wb) {
        while (wbs.blocks--) {
            hasWritten += cinquainWriteBlock(key, key_length, &wbs.wb[wbs.blocks]);
            if (errorNo)
                break;
        }
        //roll back...
        if (errorNo) {
            tErrorNo = errorNo;
            while (wbs.blocks++<blockNum && cinquainDeleteBlock(key, key_length, &wbs.wb[wbs.blocks]));
            errorNo = tErrorNo; //errorNo ? errorNo : tErrorNo;
        }
        else {
            r = cinquainReadBlock(key, key_length, &wb);
            if (r) {
                oldBlockNum = *((unsigned int *)r->str);
                freeReplyObject(r);
            }
            if (oldBlockNum < blockNum) {
                wb.buffer = (char *)&blockNum;
                cinquainWriteBlock(key, key_length, &wb);
            }
        }
        free(wbs.wb);
    }
    return !errorNo ? hasWritten : errorNo;
}

offset_t cinquainAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        const offset_t current_length,
                        const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        return bfileAppend(key, key_length, value, value_length, current_length, file_size);
    int hasWritten = 0, tErrorNo;
    workBlocks wbs = {NULL, 0};
    cinquainSetWorkBlocks(&wbs, current_length, value_length, value);
    unsigned int blockNum = wbs.wb[wbs.blocks-1].id; 
    workBlock wb = {numks, 0, keyExLength, (char *)&blockNum};
    
    if (wbs.wb) {
        while (wbs.blocks--) {
            hasWritten += cinquainWriteBlock(key, key_length, &wbs.wb[wbs.blocks]);
            if (errorNo)
                break;
        }
        //roll back...
        if (errorNo) {
            tErrorNo = errorNo;
            while (wbs.blocks++<blockNum && cinquainDeleteBlock(key, key_length, &wbs.wb[wbs.blocks]));
            errorNo = errorNo ? errorNo : tErrorNo;
        }
        else
            cinquainWriteBlock(key, key_length, &wb);
        free(wbs.wb);
    }
    return !errorNo ? hasWritten+current_length : errorNo;
}

int cinquainIncrease(const char *key, const int key_length) {
    return cinquainIncreaseBy(key, key_length, 1);
}

int cinquainDecrease(const char *key, const int key_length) {
    return cinquainIncreaseBy(key, key_length, -1);
}

int cinquainRemove(const char *key, const int key_length,
                   const offset_t file_size) {

    if (file_size > BIG_FILE_SIZE)
        return bfileRemove(key, key_length, file_size);
    workBlock wb = {numks, 0, keyExLength, NULL};
    redisReply *r = cinquainReadBlock(key, key_length, &wb);
    if (r) {
        unsigned int blockNum = *((unsigned int *)r->str);
        freeReplyObject(r);
        if (!cinquainDeleteBlock(key, key_length, &wb))
            return errorNo;
        wb.id = refks;
        if (!cinquainDeleteBlock(key, key_length, &wb))
            return errorNo;
        while(blockNum > 0){
            wb.id = blockNum--;
            if (!cinquainDeleteBlock(key, key_length, &wb))
                break;
        }
    }
    return errorNo;
}

int cinquainStrlen(const char *key, const int key_length) {
    workBlock wb = {numks, 0, keyExLength, NULL};
    redisReply *r = cinquainReadBlock(key, key_length, &wb);
    int len = 0;
    if (r) {
        wb.id = *((unsigned int *)r->str);
        freeReplyObject(r);
        len = cinquainStrlenBlock(key, key_length, &wb);
    }
    return errorNo ? errorNo : (wb.id-1) * BLOCK_SIZE + len;
}

long long cinquainUsedMemory()
{
    int i = 0, j = 0, col = 19;
    long long usedMemory = 0, m = 0;
    char buf[64], *str=NULL;
    redisReply *r = NULL;
    for (i=0; i<redisServerNum; i++) {
        if (c[i]) {
            r = redisCommand(c[i], "INFO");
	    if (r && r->type==REDIS_REPLY_STRING && r->len>0) {
                str = r->str;
                for (j=0; j<col; j++) {
                    sscanf(str, "%s", buf);
                    str += strlen(buf) + 2;  //\r\n
                }
                sscanf(str, "used_memory:%lld", &m);
                usedMemory += m;
            }
            else
                break;
            if (r)
                freeReplyObject(r);
        }
    }
    //if (errorNo && r)
    //    freeReplyObject(r);
    return errorNo ? errorNo : usedMemory;
}

static redisReply *cinquainReadBlock(const char *key, const int key_length,
                                     workBlock *wb){

    redisContext *c = cinquainGetContext(key, key_length);
    redisReply *r = NULL;
    if (c) {
        //r = redisCommand(c, "GETRANGE %b%b %u %u", key, key_length, &wb->id, keyExLength, wb->offset, wb->offset+wb->length-1);
        r = cinquainRedisCommand(c, "GETRANGE", key, key_length, (const char *)&wb->id, keyExLength, wb->offset, wb->offset+wb->length-1, NULL, 0);
	if (r && r->type==REDIS_REPLY_STRING && r->len>0) {
            if (wb->buffer)
                memcpy(wb->buffer, r->str, r->len);
        }
        else {
            if (r)
                freeReplyObject(r);
            cinquainErrLog(c, r);
            return NULL;
        }
    }
    return r;
}

static int cinquainWriteBlock(const char *key, const int key_length,
                              workBlock *wb){

    redisContext * c = cinquainGetContext(key, key_length);
    redisReply * r = NULL;
    if (c){
        //r = redisCommand(c, "SETRANGE %b%b %u %b", key, key_length,
        //                 &wb->id, keyExLength, wb->offset, wb->buffer, wb->length);
        r = cinquainRedisCommand(c, "SETRANGE", key, key_length,
                         (const char *)&wb->id, keyExLength, wb->offset, 0, wb->buffer, wb->length);
        if (!r || r->type!=REDIS_REPLY_INTEGER || r->integer!=wb->offset+wb->length)
            cinquainErrLog(c, r);
        if (r)
            freeReplyObject(r);
    }
    return errorNo ? 0 : wb->length;
}

static int cinquainAppendBlock(const char *key, const int key_length,
                              workBlock *wb){

    redisContext * c = cinquainGetContext(key, key_length);
    redisReply * r = NULL;
    if (c){
        //r = redisCommand(c, "APPEND %b%b %b", key, key_length,
        //                 &wb->id, keyExLength, wb->buffer, wb->length);
        r = cinquainRedisCommand(c, "APPEND", key, key_length,
                         (const char *)&wb->id, keyExLength, 0, 0, wb->buffer, wb->length);
        if (!r || r->type!=REDIS_REPLY_INTEGER || r->integer!=wb->offset+wb->length)
            cinquainErrLog(c, r);
        if (r)
            freeReplyObject(r);
    }
    return errorNo ? 0 : wb->length;
}

static int cinquainStrlenBlock(const char *key, const int key_length,
                               workBlock *wb){

    redisContext * c = cinquainGetContext(key, key_length);
    redisReply * r = NULL;
    int len = 0;
    if (c){
        //r = redisCommand(c, "STRLEN %b%b", key, key_length, &wb->id, keyExLength);
        r = cinquainRedisCommand(c, "STRLEN", key, key_length, (const char *)&wb->id, keyExLength, 0, 0, NULL, 0);
        if (!r || r->type!=REDIS_REPLY_INTEGER)
            cinquainErrLog(c, r);
        else
            len = r->integer;
        if (r)
            freeReplyObject(r);
    }
    return errorNo ? errorNo : len;
}

static int cinquainDeleteBlock(const char *key, const int key_length,
                               workBlock *wb){

    redisContext *c = cinquainGetContext(key, key_length);
    redisReply *r = NULL;
    if (c) {
        //r = redisCommand(c, "DEL %b%b", key, key_length, &wb->id, keyExLength);
        r = cinquainRedisCommand(c, "DEL", key, key_length, (const char *)&wb->id, keyExLength, 0, 0, NULL, 0);
	if (!r || r->type!=REDIS_REPLY_INTEGER || r->integer!=1)
            cinquainErrLog(c, r);
        if (r)
            freeReplyObject(r);
    }
    return errorNo ? 0 : 1;
}

static int cinquainIncreaseBy(const char *key, const int key_length,
                              int increment){
    redisContext *c = cinquainGetContext(key, key_length);
    redisReply *r = NULL;
    int val = 0;
    if (c) {
        //r = increment > 0 ? redisCommand(c, "INCRBY %b%b %u", key, key_length, &refks, keyExLength, increment) : redisCommand(c, "DECRBY %b%b %u", key, key_length, &refks, keyExLength, -increment);
        r = increment > 0 ? cinquainRedisCommand(c, "INCR", key, key_length, (const char *)&refks, keyExLength, 0, 0, NULL, 0) : cinquainRedisCommand(c, "DECR", key, key_length, (const char *)&refks, keyExLength, 0, 0, NULL, 0);
        if (r && r->type==REDIS_REPLY_INTEGER)
            val = r->integer;
        else
            cinquainErrLog(c, r);
        if (r)
            freeReplyObject(r);
    }
    return errorNo ? errorNo : val;
}

static redisContext *cinquainGetContext(const char *key, const int key_length){

    //clear error number & error message
    errorNo = 0;
    errorMsg[0] = '\0';
    //map the key to a server & make sure the connection not error, 'i' is the server index
    int i = (*(long long *)key) % redisServerNum;
    if (!c[i] || c[i]->err) {
        if (c[i] && c[i]->err)
            redisFree(c[i]);
        if (redisPort[i] < 0)
            c[i] = redisConnectUnix(redisIp[i]);
            //c[i] = redisConnectUnixWithTimeout(redisIp[i], timeout);
        else
            c[i] = redisConnect(redisIp[i], redisPort[i]);
            //c[i] = redisConnectWithTimeout(redisIp[i], redisPort[i], timeout);
    }

    if (c[i] && !c[i]->err)
        return c[i];
    else {
        cinquainErrLog(c[i], NULL);
        return NULL;
   }
}

static workBlocks *cinquainSetWorkBlocks(workBlocks *wbs, offset_t offset, offset_t length, const char *value){

    if (offset < 0 || length <= 0 || offset + length < offset) {
        errorNo = CINQUAIN_ERR_RANGE;
        return wbs;
    }
    unsigned int startBlock = offset / BLOCK_SIZE + 1;
    unsigned int endBlock = (offset + length - 1) / BLOCK_SIZE + 1;
    wbs->blocks = endBlock - startBlock + 1;
    int i = 0;
    wbs->wb = (workBlock *)malloc(sizeof(workBlock)*wbs->blocks);
    char *buffer, *cur;

    cur = buffer = (char *)(value ? 
                            value : (wbs->blocks > 1 ?
                                     (char *)malloc(sizeof(char)*length) : NULL));

    while (startBlock <= endBlock) {
        wbs->wb[i].id = startBlock++;
        wbs->wb[i].offset = offset % BLOCK_SIZE;
        wbs->wb[i].length = BLOCK_SIZE - wbs->wb[i].offset < length ? BLOCK_SIZE - wbs->wb[i].offset : length;
        wbs->wb[i].buffer = buffer == NULL ? NULL : cur;
        cur += wbs->wb[i].length;
        offset += wbs->wb[i].length;
        length -= wbs->wb[i++].length;
    }
    return wbs;
}

static int cinquainErrLog(redisContext *c, redisReply *r) {
    errorNo = (!r) ? -c->err : ((r->type==REDIS_REPLY_STRING && r->len==0) ?
                                CINQUAIN_ERR_NX : CINQUAIN_ERR_REPLY);
    (r && r->type==REDIS_REPLY_ERROR) ? (errorMsg[r->len]='\0',memcpy(errorMsg, r->str, r->len)) : strcpy(errorMsg, c->errstr);
    return errorNo;
}

int cinquainGetErr() {
    if (errorNo)
        printf("%d : %s\n", errorNo, errorMsg);
    return errorNo;
}

static char *bfilePath(const char *key, const int key_length) {

    memcpy(bfileRootpath+rootpathLen, key, key_length);
    bfileRootpath[rootpathLen+key_length] = 0;
    return bfileRootpath;
}
static char **bfileReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length,
                         const offset_t file_size) {

    bfileBuffer = NULL;
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
    return &bfileBuffer;
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

static redisReply *cinquainRedisCommand(redisContext *c, char *cmd, const char *key, const int key_length, const char *key_ex, const int key_ex_length, offset_t start, offset_t end, char *value, offset_t value_length) {
    int argc = 2;
    char *argv[ARGV_ARRAY_LEN];
    int argvlen[ARGV_ARRAY_LEN];
    char rkey[KEY_LEN];
    char sbuf[BUF_LEN];
    char ebuf[BUF_LEN];
    argv[0] = cmd;
    argvlen[0] = strlen(cmd);
    memcpy(rkey, key, key_length);
    memcpy(rkey+key_length, key_ex, key_ex_length);
    rkey[key_length+key_ex_length] = '\0';
    argv[1] = rkey;
    argvlen[1] = key_length + key_ex_length;
    if (strcmp(cmd, "GETRANGE") == 0) {
        sprintf(sbuf, "%u", start);
        argv[2] = sbuf;
        argvlen[2] = strlen(sbuf);
        sprintf(ebuf, "%u", end);
        argv[3] = ebuf;
        argvlen[3] = strlen(ebuf);
        argc = 4;
    }
    else if (strcmp(cmd, "SETRANGE") == 0) {
        sprintf(sbuf, "%u", start);
        argv[2] = sbuf;
        argvlen[2] = strlen(sbuf);
        argv[3] = value;
        argvlen[3] = value_length;
        argc = 4;
    }
    else if (strcmp(cmd, "APPEND") == 0) {
        argv[2] = value;
        argvlen[2] = value_length;
        argc = 3;
    }
    return redisCommandArgv(c, argc, (const char **)argv, argvlen);
}
