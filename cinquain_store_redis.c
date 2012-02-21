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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cinquain_store.h"
#include "hiredis/hiredis.h"


// redis context & reply handler
redisContext *c[128];
redisReply *reply[128];
redisServerNum;

int cinquainInitBackStore(const int argc, const char *argv[]) {
    
    struct timeval timeout = {1, 500000}; // 1.5 seconds
    char *ipAddress;
    unsigned int port;
    //read redis server config & get connections
    FILE *fp = fopen(SERVER_CONFIG, "r");
    while(!feof(fp)) {
        fscanf(fp, "%s %d\n", ipAddress, &port);
        c[redisServerNum] = redisConnectWithTimeout(ipAddress, port, timeout);
        c[redisServerNum]->err ? 0:redisServerNum++;
    }
    fclose(fp);
    return redisServerNum;
}

char **cinquainReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length) {
    int i = key[0] % redisServerNum;
    // append one byte to key for extend blocks, limit file size < 128 GB.
    char block = offset / BLOCK_SIZE + 1;
    reply[i] = redisCommand(c[i], "GETRANGE %s%c %u %u", key, block, offset, offset+BLOCK_SIZE);
    return &reply->str;
}

int cinquainDeleteBufferHost(const char **value) {
    freeReplyObject(cinquainBufferHost(value, redisReply, str));
    return 0;
}

int cinquainWriteRange(const char *key, const int key_length,
                       const char *value, const int value_length) {
  return 0;
}

offset_t cinquainAppend(const char *key, const int key_length,
                        const char *value, const int value_length,
                        offset_t current_length) {
  return 0;
}

int cinquainIncrease(const char *key, const int key_length) {
  return 0;
}

int cinquainDecrease(const char *key, const int key_length) {
  return 0;
}

int CinquainRemove(const char *key, const int key_length) {
  return 0;
}


