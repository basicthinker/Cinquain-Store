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

// const varibles
// in bytes
//#define BLOCK_SIZE (512*1024*1024) 
#define BLOCK_SIZE (128*1024*1024) 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cinquain_store.h"
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
static redisContext *cinquainGetContext(const char *key, const int key_length);
static work_blocks *cinquainSetWorkBlocks(work_blocks *wbs, offset_t offset, offset_t length, const char *value);
static redisReply *cinquainReadBlock(const char *key, const int key_length, work_block *wb);
static int cinquainWriteBlock(const char *key, const int key_length, work_block *wb);
static int cinquainDeleteBlock(const char *key, const int key_length, work_block *wb);
static int cinquainStrlenBlock(const char *key, const int key_length, work_block *wb);
static int cinquainIncreaseBy(const char *key, const int key_length, int increment);
static int cinquainErrLog(redisContext *c, redisReply *r);

int cinquainInitBackStore(const int argc, const char *argv[]) {
    
    char ip[64];
    int port, i=0;

    //read redis server config & get connections
    FILE *fp = fopen(argv[1], "r");
    if (fp != NULL) {
        while(!feof(fp)) {
            fscanf(fp, "%s %d\n", ip, &port);
            if (port < 0)
                //c[i] = redisConnectUnix(ip);
                c[i] = redisConnectUnixWithTimeout(ip, timeout);
            else
                //c[i] = redisConnect(ip, port);
                c[i] = redisConnectWithTimeout(ip, port, timeout);
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
                         const offset_t offset, const offset_t length) {

    redisReply *r = NULL;
    work_blocks wbs = {NULL, 0};
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

int cinquainDeleteBufferHost(const char **value) {
    freeReplyObject(cinquainBufferHost(value, redisReply, str));
    return 0;
}

int cinquainWriteRange(const char *key, const int key_length,
                       offset_t offset,
                       const char *value, const offset_t value_length) {
    int hasWritten = 0;
    work_blocks wbs = {NULL, 0};
    cinquainSetWorkBlocks(&wbs, offset, value_length, value);
    unsigned int blockNum = wbs.wb[wbs.blocks-1].id; 
    work_block wb = {numks, 0, keyExLength, (char *)&blockNum};
    /*
    redisReply *r = NULL;
    r = cinquainReadBlock(key, key_length, &wb);
    unsigned char oldBlockNum = r ? r->str[0] : 0;
    if (r)
        freeReplyObject(r);

    wb.buffer = oldBlockNum > blockNum ? &oldBlockNum : &blockNum;
    */
    int tNo;
    if (wbs.wb) {
        while (wbs.blocks--) {
            hasWritten += cinquainWriteBlock(key, key_length, &wbs.wb[wbs.blocks]);
            if (errorNo)
                break;
        }
        //roll back...
        if (errorNo) {
            //tNo = errorNo;
            //while (wbs.blocks++<blockNum && cinquainDeleteBlock(key, key_length, &wbs.wb[wbs.blocks]));
            //errorNo = errorNo ? errorNo : tNo;
        }
        else //if(cinquainStrlen(key, key_length) == offset + value_length)
            cinquainWriteBlock(key, key_length, &wb);
        free(wbs.wb);
    }
    return !errorNo ? hasWritten : errorNo;
}

offset_t cinquainAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        offset_t current_length) {
    int len = cinquainStrlen(key, key_length);
    len = len < 0 ? 0 : len;
    int hasWritten = cinquainWriteRange(key, key_length, len, value, value_length);
    return hasWritten > 0 ? hasWritten + len : hasWritten;
}

int cinquainIncrease(const char *key, const int key_length) {
    return cinquainIncreaseBy(key, key_length, 1);
}

int cinquainDecrease(const char *key, const int key_length) {
    return cinquainIncreaseBy(key, key_length, -1);
}

int cinquainRemove(const char *key, const int key_length) {
    work_block wb = {numks, 0, keyExLength, NULL};
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
    work_block wb = {numks, 0, keyExLength, NULL};
    redisReply *r = cinquainReadBlock(key, key_length, &wb);
    int len = 0;
    if (r) {
        wb.id = *((unsigned int *)r->str);
        freeReplyObject(r);
        len = cinquainStrlenBlock(key, key_length, &wb);
    }
    return errorNo ? errorNo : (wb.id-1) * BLOCK_SIZE + len;
}

long long cinquainUsedMemoryRss()
{
    int i = 0, j = 0, col = 21;
    long long usedMemoryRss = 0, m = 0;
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
                sscanf(str, "used_memory_rss:%lld", &m);
                usedMemoryRss += m;
            }
            else
                break;
            if (r)
                freeReplyObject(r);
        }
    }
    if (errorNo && r)
        freeReplyObject(r);
    return errorNo ? errorNo : usedMemoryRss;
}

static redisReply *cinquainReadBlock(const char *key, const int key_length, work_block *wb){

    redisContext *c = cinquainGetContext(key, key_length);
    redisReply *r = NULL;
    if (c) {
        r = redisCommand(c, "GETRANGE %b%b %u %u", key, key_length, &wb->id, keyExLength, wb->offset, wb->offset+wb->length-1);
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

static int cinquainWriteBlock(const char *key, const int key_length, work_block *wb){

    redisContext * c = cinquainGetContext(key, key_length);
    redisReply * r = NULL;
    if (c){
        r = redisCommand(c, "SETRANGE %b%b %u %b", key, key_length, &wb->id, keyExLength, wb->offset, wb->buffer, wb->length);
        //r = redisCommand(c, "SETRANGE foo 0 bar");
        if (!r || r->type!=REDIS_REPLY_INTEGER || r->integer!=wb->offset+wb->length)
            cinquainErrLog(c, r);
        if (r)
            freeReplyObject(r);
    }
    return errorNo ? 0 : wb->length;
}

static int cinquainStrlenBlock(const char *key, const int key_length, work_block *wb){

    redisContext * c = cinquainGetContext(key, key_length);
    redisReply * r = NULL;
    int len = 0;
    if (c){
        r = redisCommand(c, "STRLEN %b%b", key, key_length, &wb->id, keyExLength);
        if (!r || r->type!=REDIS_REPLY_INTEGER)
            cinquainErrLog(c, r);
        else
            len = r->integer;
        if (r)
            freeReplyObject(r);
    }
    return errorNo ? errorNo : len;
}

static int cinquainDeleteBlock(const char *key, const int key_length, work_block *wb){

    redisContext *c = cinquainGetContext(key, key_length);
    redisReply *r = NULL;
    if (c) {
        r = redisCommand(c, "DEL %b%b", key, key_length, &wb->id, keyExLength);
	if (!r || r->type!=REDIS_REPLY_INTEGER || r->integer!=1)
            cinquainErrLog(c, r);
        if (r)
            freeReplyObject(r);
    }
    return errorNo ? 0 : 1;
}

static int cinquainIncreaseBy(const char *key, const int key_length, int increment){

    redisContext *c = cinquainGetContext(key, key_length);
    redisReply *r = NULL;
    int val = 0;
    if (c) {
        r = increment > 0 ? redisCommand(c, "INCRBY %b%b %u", key, key_length, &refks, keyExLength, increment) : redisCommand(c, "DECRBY %b%b %u", key, key_length, &refks, keyExLength, -increment);
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

static work_blocks *cinquainSetWorkBlocks(work_blocks *wbs, offset_t offset, offset_t length, const char *value){

    if (offset < 0 || length <= 0 || offset + length < offset) {
        errorNo = CINQUAIN_ERR_RANGE;
        return wbs;
    }
    unsigned int startBlock = offset / BLOCK_SIZE + 1;
    unsigned int endBlock = (offset + length - 1) / BLOCK_SIZE + 1;
    wbs->blocks = endBlock - startBlock + 1;
    int i = 0;
    wbs->wb = (work_block *)malloc(sizeof(work_block)*wbs->blocks);
    char *buffer, *cur;

    cur = buffer = (char *)(value ? value : (wbs->blocks > 1 ? (char *)malloc(sizeof(char)*length) : NULL));

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
    errorNo = (!r) ? -c->err : ((r->type==REDIS_REPLY_STRING && r->len==0) ? CINQUAIN_ERR_NX : CINQUAIN_ERR_REPLY);
    (r && r->type==REDIS_REPLY_ERROR) ? (errorMsg[r->len]='\0',memcpy(errorMsg, r->str, r->len)) : strcpy(errorMsg, c->errstr);
    return errorNo;
}

int cinquainGetErr(){
    if (errorNo)
        printf("%d : %s\n", errorNo, errorMsg);
    return errorNo;
}
