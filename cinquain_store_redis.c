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
#define BLOCK_SIZE (512*1024*1024) // bytes

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

int cinquainInitBackStore(const int argc, const char *argv[]) {
    
    struct timeval timeout = {1, 500000}; // 1.5 seconds
    char ip[16];
    unsigned int port;
    int i=0;
    // server config param must be given 
    if (argc < 2)
        return 0;
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
    int i = key[0] % redisServerNum;
    // append one byte to key for extend blocks, limit file size < 127 GB.
    char startBlock = offset / BLOCK_SIZE + 1;
    char currentBlock = startBlock;
    char endBlock = (offset + length) / BLOCK_SIZE + 1;
    char *buffer = malloc(sizeof(char)*length);
    char *cur = buffer;
    int start = offset % BLOCK_SIZE;
    int end;

    //pipeline commands
    while (currentBlock < endBlock) {
        end = offset+length-(currentBlock-startBlock)*BLOCK_SIZE;
        end = (end<BLOCK_SIZE) ? end : BLOCK_SIZE;
        redisAppendCommand(c[i], "GETRANGE %s%c %u %u", key, currentBlock++, start, end);
        start = 0;
    }

    //handle replies
    redisGetReply(c[i], &reply[i]);
    while (startBlock<endBlock && reply[i]->type!=REDIS_REPLY_ERROR) {
        memcpy(cur, reply[i]->str, reply[i]->len);
        cur += sizeof(char) * reply[i]->len;
        startBlock++;
        redisGetReply(c[i], &reply[i]);
    }

    reply[i]->type==REDIS_REPLY_ERROR ? (free(buffer), buffer=NULL, cur=NULL) : memcpy(cur, reply[i]->str, reply[i]->len);

    free(reply[i]->str);
    reply[i]->str = buffer;
    // reconnect redis server as error occurs
    c[i] = (reply[i]->type!=REDIS_REPLY_ERROR ? c[i] : redisConnect(redisIp[i], redisPort[i]));
    freeReplyObject(reply[i]);

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
    offset_t len = 0;
    const char *cur = value;
    // append one byte to key for extend blocks, limit file size < 127 GB.
    char startBlock = offset / BLOCK_SIZE + 1;
    char currentBlock = startBlock;
    char endBlock = (offset + value_length) / BLOCK_SIZE + 1;
    int blockNum;
    //write value in 512 MB BLOCK one by one 
    //pipeline commands
    redisAppendCommand(c[i], "SETRANGE %s%c %u %s", key, currentBlock++, offset, cur);
    while (currentBlock <= endBlock) {
        cur = value + (currentBlock-startBlock) * BLOCK_SIZE - offset;
        redisAppendCommand(c[i], "SET %s%c %s", key, currentBlock++, cur);
    }
    //set block num
    redisAppendCommand(c[i], "SET %s%c %s", key, 0xff, endBlock);

    //handle replies
    while (startBlock <= endBlock) {
        redisGetReply(c[i], &reply[i]);
        if (reply[i]->type != REDIS_REPLY_ERROR)
            len += reply[i]->integer;
        else {
            freeReplyObject(reply[i]);
            break;
        }
        freeReplyObject(reply[i]);
        startBlock++;
    }
    // handle the set block num command
    if (startBlock > endBlock) {
        redisGetReply(c[i], &reply[i]);
        freeReplyObject(reply[i]);
    }

    // reconnect redis server as error occurs
    c[i] = (reply[i]->type!=REDIS_REPLY_ERROR ? c[i] : redisConnect(redisIp[i], redisPort[i]));
    freeReplyObject(reply[i]);

    return len;
}

offset_t cinquainAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        offset_t current_length) {
     return cinquainWriteRange(key, key_length, current_length, value, value_length) + current_length;
}

int cinquainIncrease(const char *key, const int key_length) {
    int i = key[0] % redisServerNum, ret;
    reply[i] = redisCommand(c[i], "INCR %s%c", key, 0x00);
    ret = reply[i]->integer;

    // reconnect redis server as error occurs
    c[i] = (reply[i]->type!=REDIS_REPLY_ERROR ? c[i] : redisConnect(redisIp[i], redisPort[i]));
    freeReplyObject(reply[i]);

    return ret;
}

int cinquainDecrease(const char *key, const int key_length) {
    int i = key[0] % redisServerNum, ret;
    reply[i] = redisCommand(c[i], "DECR %s%c", key, 0x00);
    ret = reply[i]->integer;

    // reconnect redis server as error occurs
    c[i] = (reply[i]->type!=REDIS_REPLY_ERROR ? c[i] : redisConnect(redisIp[i], redisPort[i]));
    freeReplyObject(reply[i]);

    return ret;
}

int CinquainRemove(const char *key, const int key_length) {
    int i = key[0] % redisServerNum;
    int blockNum, replyNum;
    reply[i] = redisCommand(c[i], "GET %s%c", key, 0xff);
    blockNum = reply[i]->type!=REDIS_REPLY_ERROR ? reply[i]->integer : 0;
    replyNum = blockNum + 2;
    freeReplyObject(reply[i]);
    while (blockNum > 0)
        redisAppendCommand(c[i], "DEL %s%c", key, blockNum--);
    redisAppendCommand(c[i], "DEL %s%c", key, 0xff);
    redisAppendCommand(c[i], "DEL %s%c", key, 0x00);

    while (replyNum-- > 0) {
        redisGetReply(c[i], &reply[i]);
        if (reply[i]->type == REDIS_REPLY_ERROR)
            break;
        freeReplyObject(reply[i]);
    }

    // reconnect redis server as error occurs
    c[i] = (reply[i]->type!=REDIS_REPLY_ERROR ? c[i] : redisConnect(redisIp[i], redisPort[i]));
    freeReplyObject(reply[i]);

    return replyNum;
}
