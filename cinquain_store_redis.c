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
#define SERVER_CONFIG redis_server.config
#define BLOCK_SIZE (512*1024*1024) // bytes
// MACRO for max & min
#define max(a,b) \
    ({ typeof (a) _a = (a); \
        typeof (b) _b = (b); \
      _a > _b ? _a : _b; })
#define min(a,b) \
    ({ typeof (a) _a = (a); \
        typeof (b) _b = (b); \
      _a < _b ? _a : _b; })

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cinquain_store.h"
#include "hiredis/hiredis.h"


// redis context & reply handler
redisContext *c[256];
redisReply *reply[256];
redisServerNumi = 0;

int cinquainInitBackStore(const int argc, const char *argv[]) {
    
    struct timeval timeout = {1, 500000}; // 1.5 seconds
    char *ipAddress;
    unsigned int port;
    //read redis server config & get connections
    FILE *fp = fopen(SERVER_CONFIG, "r");
    while(!feof(fp)) {
        fscanf(fp, "%s %u\n", ipAddress, &port);
        c[redisServerNum] = redisConnectWithTimeout(ipAddress, port, timeout);
        c[redisServerNum]->err ? 0:redisServerNum++;
    }
    fclose(fp);
    return redisServerNum;
}

char **cinquainReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length) {
    int i = key[0] % redisServerNum;
    // append one byte to key for extend blocks, limit file size < 127 GB.
    char currentBlockstartBlock = offset / BLOCK_SIZE + 1;
    char endBlock = (offset + length) / BLOCK_SIZE + 1;
    char * buffer = malloc(sizeof(char)*length);
    reply[i] = redisCommand(c[i], "GETRANGE %s%c %u %u", key, currentBlock++, offset, offset+length);

    reply[i]->type==REDIS_REPLY_ERROR ? (free(buffer), buffer=NULL) : strcpy(buffer, reply[i]->str);

    while (currentBlock<=endBlock && reply[i]->type!=REDIS_REPLY_ERROR) {
        freeReplyObject(reply[i]);
        reply[i] = redisCommand(c[i], "GETRANGE %s%c %u %u", key, currentBlock, 0, offset+length-(currentBlock-startBlock)*BLOCK_SIZE);
        strcpy(buffer + (currentBlock - startBlock) * BLOCK_SIZE - offset, reply[i]->str);
        currentBlock++;
    }
    free(reply[i]->str);
    reply[i]->str = buffer;
    return &(reply[i]->str);
}

int cinquainDeleteBufferHost(const char **value) {
    freeReplyObject(cinquainBufferHost(value, redisReply, str));
    return 0;
}

int cinquainWriteRange(const char *key, const int key_length,
                       offset_t offset,
                       const char *value, const offset_t value_length) {
    int i = key[0] % redisServerNum;
    offset_t len;
    char *cur = value;
    // append one byte to key for extend blocks, limit file size < 127 GB.
    char currentBlock = startBlock = offset / BLOCK_SIZE + 1;
    char endBlock = (offset + value_length) / BLOCK_SIZE + 1;
    //write value in 512 MB BLOCK one by one 
    reply[i] = redisCommand(c[i], "SETRANGE %s%c %u %s", key, currentBlock++, offset, cur);
    while (currentBlock<=endBlock && reply[i]->type!=REDIS_REPLY_ERROR) {
        freeReplyObject(reply[i]);
        cur = value + (currentBlock-startBlock) * BLOCK_SIZE - offset;
        reply[i] = redisCommand(c[i], "SET %s%c %s", key, currentBlock++, cur);
    }
    freeReplyObject(reply[i]);
    //record the BLOCK num
    reply[i] = redisCommand(c[i], "SET %s%c %s", key, 0xff, currentBlock);
    freeReplyObject(reply[i]);

    len = (currentBlock-1-startBlock) * BLOCK_SIZE - offset;
    return max(0, min(len, value_length));
}

offset_t cinquainAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        offset_t current_length) {
     
    int i = key[0] % redisServerNum;
    offset_t len;
    int replyType;
    char *cur = value;
    //add ref count for new key
    if (current_length == 0) {
        reply[i] = redisCommand(c[i], "SET %s%c %d", key, 0x00, 0);
        replyType = reply[i]->type;
        freeReplyObject(reply[i]);
        if (replyType == REDIS_REPLY_ERROR)
            return current_length;
    }
        
    // append one byte to key for extend blocks, limit file size < 127 GB.
    char currentBlock = startBlock = current_length / BLOCK_SIZE + 1;
    char endBlock = (current_length + value_length) / BLOCK_SIZE + 1;
    //write value in 512 MB BLOCK one by one 
    reply[i] = redisCommand(c[i], "APPEND %s%c %s", key, currentBlock++, cur);
    while (currentBlock<=endBlock && reply[i]->type!=REDIS_REPLY_ERROR) {
        freeReplyObject(reply[i]);
        cur = value + (currentBlock-startBlock) * BLOCK_SIZE - current_length;
        reply[i] = redisCommand(c[i], "SET %s%c %s", key, currentBlock++, cur);
    }
    freeReplyObject(reply[i]);

    //record the BLOCK num
    reply[i] = redisCommand(c[i], "SET %s%c %s", key, 0xff, currentBlock);
    freeReplyObject(reply[i]);

    len = (currentBlock-1-startBlock) * BLOCK_SIZE - current_length;
    return max(0, min(len, value_length)) + current_length;
}

int cinquainIncrease(const char *key, const int key_length) {
    int i = key[0] % redisServerNum;
    reply[i] = redisCommand(c[i], "INCR %s%c", key, 0x00);
    return reply[i]->integer;
}

int cinquainDecrease(const char *key, const int key_length) {
    int i = key[0] % redisServerNum;
    reply[i] = redisCommand(c[i], "DECR %s%c", key, 0x00);
    return reply[i]->integer;
}

int CinquainRemove(const char *key, const int key_length) {
    int i = key[0] % redisServerNum;
    
    reply[i] = redisCommand(c[i], "GET %s%c", key, 0xff);
    int block_num = reply[i]->type!=REDIS_REPLY_ERROR ? reply[i]->integer : 0;
    while (block_num-- > 0)
        reply[i] = redisCommand(c[i], "DEL %s%c", key, block_num+1);
    reply[i] = redisCommand(c[i], "DEL %s%c", key, 0xff);
    reply[i] = redisCommand(c[i], "DEL %s%c", key, 0x00);
    return block_num;
}


