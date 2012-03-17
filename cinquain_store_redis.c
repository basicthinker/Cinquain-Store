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
#define BLOCK_SIZE (512*1024*1024) 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cinquain_store.h"
#include "hiredis/hiredis.h"

// redis context & reply handler
redisContext *c[256];
redisReply *reply[256];
// redis server config
char *redisIp[256];
unsigned int redisPort[256];
unsigned int redisServerNum = 0;
// refks is a key suffix to record reference & numks is a key suffix to record block number
char numks = 0xff;
char refks = 0x00;

//make sure argc < 2 if you want use default redis server config
int cinquainInitBackStore(const int argc, const char *argv[]) {
    
    struct timeval timeout = {1, 500000}; // 1.5 seconds
    char ip[16];
    unsigned int port;
    unsigned int i=0;
    // server config param , if not given redis_server.config as default
    if (argc < 2)
        argv[1] = "redis_server.config";
    //read redis server config & get connections
    FILE *fp = fopen(argv[1], "r");
    while(!feof(fp)) {
        fscanf(fp, "%s %u\n", ip, &port);
        c[i] = redisConnectWithTimeout(ip, port, timeout);
        if (!c[i]->err) {
            redisIp[i] = malloc(sizeof(char) * (strlen(ip) + 1 ));
            strcpy(redisIp[i], ip);
            redisPort[i++] = port;
        }
    }
    fclose(fp);
    redisServerNum = i;
    return redisServerNum;
}

char **cinquainReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length) {
    // append one byte to key for extend blocks, limit file size < 127 GB.
    char startBlock = offset / BLOCK_SIZE + 1;
    char currentBlock = startBlock;
    char endBlock = (offset + length - 1) / BLOCK_SIZE + 1;
    char *buffer = malloc(sizeof(char)*length);
    char *cur = buffer;
    //range is [start, end]
    int offsetInBlock = offset % BLOCK_SIZE;
    int start=offsetInBlock;
    int end;
    int replyState = REDIS_OK;
    //map the key to a server & make sure the connection not error, 'i' is the server index
    int i = key[0] % redisServerNum;
    if (c[i]->err)
        c[i] = redisConnect(redisIp[i], redisPort[i]);

    //pipeline commands
    while (currentBlock <= endBlock) {
        end = offsetInBlock+length-1-(currentBlock-startBlock)*BLOCK_SIZE;
        end = (end<BLOCK_SIZE-1) ? end : BLOCK_SIZE-1;
        //redisAppendCommand(c[i], "GET foo");//%b", key, key_length);
        redisAppendCommand(c[i], "GETRANGE %b%b %u %u", key, key_length, &currentBlock, 1, start, end);
        //printf("GETRANGE %s%c %u %u\n", key, currentBlock, start, end);
        currentBlock++;
        start = 0;
    }

    //handle replies
    replyState = redisGetReply(c[i], (void **)&reply[i]);

    while (startBlock<=endBlock && replyState!=REDIS_ERR) {
        memcpy(cur, reply[i]->str, reply[i]->len);
        cur += sizeof(char) * reply[i]->len;
        //printf("str %s\n", reply[i]->str);
        //printf("len %d\n", reply[i]->len);
        startBlock++;
        if (startBlock <= endBlock) {
            freeReplyObject(reply[i]);
            replyState = redisGetReply(c[i], (void **)&reply[i]);
        }
    }
    //reply error ...
    cur-buffer != length ? (free(buffer), buffer=NULL, cur=NULL) : 0;

    free(reply[i]->str);
    reply[i]->str = buffer;

    //printf("%s", reply[i]->str);
    return &(reply[i]->str);
}

int cinquainDeleteBufferHost(const char **value) {
    freeReplyObject(cinquainBufferHost(value, redisReply, str));
    return 0;
}

int cinquainWriteRange(const char *key, const int key_length,
                       offset_t offset,
                       const char *value, const offset_t value_length) {
    offset_t len = 0;
    const char *cur = value;
    // append one byte to key for extend blocks, limit file size < 127 GB.
    char startBlock = offset / BLOCK_SIZE + 1;
    char currentBlock = startBlock;
    char endBlock = (offset + value_length) / BLOCK_SIZE + 1;
    char blockNum;
    offset_t offsetInBlock = offset % BLOCK_SIZE;
    offset_t value_len = value_length;
    offset_t length = value_length < BLOCK_SIZE-offsetInBlock ? value_length : BLOCK_SIZE-offsetInBlock;
    int writtenInBlock[256];
    writtenInBlock[currentBlock] = length;
    //map the key to a server & make sure the connection not error, 'i' is the server index
    int i = key[0] % redisServerNum;
    if (c[i]->err)
        c[i] = redisConnect(redisIp[i], redisPort[i]);
    //write value in 512 MB BLOCK one by one 
    //pipeline commands
    redisAppendCommand(c[i], "SETRANGE %b%b %u %b", key, key_length, &currentBlock, 1, offsetInBlock, cur, length);
    currentBlock++;
    value_len -= length;
    while (currentBlock <= endBlock) {
        cur = value + (currentBlock-startBlock) * BLOCK_SIZE - offsetInBlock;
        length = (value_len < BLOCK_SIZE ? value_len : BLOCK_SIZE);
        writtenInBlock[currentBlock] = length;
        redisAppendCommand(c[i], "SETRANGE %b%b %u %b", key, key_length, &currentBlock, 1, 0, cur, length);
        currentBlock++;
        value_len -= length;
    }

    //handle replies
    currentBlock = startBlock;
    while (currentBlock<=endBlock && redisGetReply(c[i], (void **)&reply[i])==REDIS_OK) {
        if (reply[i]->type == REDIS_REPLY_INTEGER)
            len += writtenInBlock[currentBlock];
        //printf("int %lld\n", reply[i]->integer);
        freeReplyObject(reply[i]);
        currentBlock++;
    }

    //if (currentBlock <= endBlock)
    //    freeReplyObject(reply[i]);
    //set block num
    //reply[i] = redisCommand(c[i], "SET %b%b 1", key, key_length, &numks, 1);
    reply[i] = redisCommand(c[i], "GET %b%b", key, key_length, &numks, 1);
    if (reply[i] && reply[i]->type == REDIS_REPLY_STRING) {
        blockNum = startBlock-1 > reply[i]->str[0] ? startBlock-1 : reply[i]->str[0];
    }
    else
        blockNum = startBlock-1;
    if (reply[i])
        freeReplyObject(reply[i]);
    reply[i] = redisCommand(c[i], "SET %b%b %b", key, key_length, &numks, 1, &blockNum, 1);
    if (reply[i])
        freeReplyObject(reply[i]);

    return len;
}

offset_t cinquainAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        offset_t current_length) {
     return cinquainWriteRange(key, key_length, current_length, value, value_length)+current_length;
}

int cinquainIncrease(const char *key, const int key_length) {
    int ret = 0;
    //map the key to a server & make sure the connection not error, 'i' is the server index
    int i = key[0] % redisServerNum;
    if (c[i]->err)
        c[i] = redisConnect(redisIp[i], redisPort[i]);

    reply[i] = redisCommand(c[i], "INCR %b%b", key, key_length, &refks, 1);
    ret = reply[i]->integer;

    freeReplyObject(reply[i]);

    return ret;
}

int cinquainDecrease(const char *key, const int key_length) {
    int ret = 0;
    //map the key to a server & make sure the connection not error, 'i' is the server index
    int i = key[0] % redisServerNum;
    if (c[i]->err)
        c[i] = redisConnect(redisIp[i], redisPort[i]);

    reply[i] = redisCommand(c[i], "DECR %b%b", key, key_length, &refks, 1);
    ret = reply[i]->integer;

    freeReplyObject(reply[i]);

    return ret;
}

int cinquainRemove(const char *key, const int key_length) {
    char blockNum = 0;
    int replyNum;
    //map the key to a server & make sure the connection not error, 'i' is the server index
    int i = key[0] % redisServerNum;
    if (c[i]->err)
        c[i] = redisConnect(redisIp[i], redisPort[i]);

    reply[i] = redisCommand(c[i], "GET %b%b", key, key_length, &numks, 1);
    if (reply[i]->type == REDIS_REPLY_STRING)
        blockNum = reply[i]->str[0];
    replyNum = blockNum + 2;
    freeReplyObject(reply[i]);
    while (blockNum > 0) {
        redisAppendCommand(c[i], "DEL %b%b", key, key_length, &blockNum, 1);
        blockNum--;
    }
    redisAppendCommand(c[i], "DEL %b%b", key, key_length, &numks, 1);
    redisAppendCommand(c[i], "DEL %b%b", key, key_length, &refks, 1);
    //handle replies
    while (replyNum>0 && redisGetReply(c[i], (void **)&reply[i])==REDIS_OK) {
        freeReplyObject(reply[i]);
        replyNum--;
    }
    //if (replyNum > 0)
    //    freeReplyObject(reply[i]);
    return replyNum;
}

//you can only use these redis-pre function for test!!!
char *redisRead(const char *key, const int key_length)
{
    //map the key to a server & make sure the connection not error, 'i' is the server index
    int i = key[0] % redisServerNum;
    char *buffer = NULL;
    if (c[i]->err)
        c[i] = redisConnect(redisIp[i], redisPort[i]);
    reply[i] = redisCommand(c[i], "GET %b", key, key_length);
    //printf("r %d\n", reply[i]->str[0]);
    if (reply[i]->type == REDIS_REPLY_STRING) {
        buffer = malloc(sizeof(char)*reply[i]->len);
        memcpy(buffer, reply[i]->str, reply[i]->len);
    }
    freeReplyObject(reply[i]);
    return buffer;
}

