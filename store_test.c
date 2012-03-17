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
//  store_test.c
//  Cinquain-Store
//
//  Created by Jinglei Ren <jinglei.ren@gmail.com> on 2/16/12.
//  Modified by Weichao Guo <guoweichao2010@gmail.com>.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <unistd.h>

#include "cinquain_store.h"
#include "store_test.h"

#define M (1024*1024)

double write_test(char *key, int range, FILE *fp, long long *fsize);
double read_test(char *key, int range, FILE *fp, long long fsize);
int str_cmp(const char *src, const char *dest, int len);

int main (int argc, const char *argv[])
{
    //args : config testfilename range(KB) ntimes
    if (argc != 5)
        return 0;
    //init
    int serverNum = cinquainInitBackStore(argc, argv);
    if (serverNum == 0)
        return 0;
    const char *testFile = argv[2];
    int range=0, ntimes=0;
    long long fsize=0;
    int i, j;
    // 20 bytes key extends 1 byte...
    char key[22] = {0xda,0x39,0xa3,0xee,0x5e,0x6b,0x4b,0x0d,0x32,0x55,0xbf,0xef,0x95,0x60,0x18,0x90,0xaf,0xd8,0x07,0x10,0xff};
    // gen different key for each process
    key[1] = getpid()%256;
    key[2] = time(0)%256;
    //printf("pid %d\n", getpid());
    //throughput MB/s
    double readThroughput = 0;
    double writeThroughput = 0;

    sscanf(argv[3], "%d", &range);
    sscanf(argv[4], "%d", &ntimes);
    //KB
    range *= 1024;
    //printf("TEST CONFIG:\n");
    //printf("server num : %d\n", serverNum);
    //printf("test file : %s\n", testFile);
    //printf("range size : %d\n", range);
    //printf("times : %d\n", ntimes);

    FILE *fp = fopen(testFile, "rb");
    if (fp == NULL)
        return 0;
    //test ntimes with all servers
    for (i=0; i<ntimes; i++)
        for (j=0; j<serverNum; j++) {
        //write then read
        key[0] = j+1;
        writeThroughput += write_test(key, range, fp, &fsize);
        fseek(fp, 0L, SEEK_SET);
        readThroughput += read_test(key, range, fp, fsize);
        fseek(fp, 0L, SEEK_SET);
    }
    fclose(fp);
    // in MB 
    writeThroughput /= ntimes*serverNum*M;
    readThroughput /= ntimes*serverNum*M;
    printf("%lf %lf\n", writeThroughput, readThroughput);
    return 0;
}

double write_test(char *key, int range, FILE *fp, long long *fsize)
{

    struct timeval start, end;
    double span = 0;
    double throughput = 0;

    char *buffer = malloc(sizeof(char)*range);
    offset_t offset = 0;
    
    long long size = range;
    long long count = 0;
    //remove first, if success return 0 
    //printf("remove return %d\n", cinquainRemove(key, 20));
    cinquainRemove(key, 20);
    while (size == range) {
        size = fread(buffer, sizeof(char), range, fp);
        count += size;
        gettimeofday(&start, NULL);
        offset = cinquainAppend(key, 20, buffer, size, offset);
        gettimeofday(&end, NULL);
        span += 1000000*(end.tv_sec-start.tv_sec) + end.tv_usec - start.tv_usec;
    }
    span /= 1000000;
    throughput = count/span;
    //printf("%u Bytes has written in %lf seconds.\nThroughoutput is %lf B/s\n", count, span, throughput);
    free(buffer);
    *fsize = count;
    return throughput;
}

//read a key in range
double read_test(char *key, int range, FILE *fp, long long size)
{
    // time span
    struct timeval start, end;
    double span = 0;
    double throughput; 

    const char **readReply;
    offset_t offset = 0;
    char *buffer = malloc(sizeof(char)*range);
    
    long long count = 0;
    span = 0;
    
    while (size > 0){ 
        count = size < range ? size : range;
        size -= count;
        //fread(buffer, sizeof(char), count, fp);
        gettimeofday(&start, NULL);
        readReply = (const char **)cinquainReadRange(key, 20, offset, count);
        gettimeofday(&end, NULL);
        //if (str_cmp(buffer, *readReply, count) != 0) {
        //    printf("!!!WRITE!=READ!!!\n");
        //    break;
        //}
        offset += count;
        cinquainDeleteBufferHost(readReply);
        span += 1000000*(end.tv_sec-start.tv_sec) + end.tv_usec - start.tv_usec;
    }
    span /= 1000000;
    throughput = offset/span;
    //printf("%u Bytes has read in %lf seconds.\nThroughoutput is %lf B/s\n", offset, span, throughput);
    free(buffer);
    return throughput;
}

int str_cmp(const char *src, const char *dest, int len)
{
    assert(NULL!=dest && NULL!=src && len>0);
    while(--len && *dest==*src) {
        dest++;
        src++;
    }
    return *dest - *src;
}
