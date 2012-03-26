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

#define M (1024*1024)

void function_test(int argc, const char *argv[]);
void test(int argc, const char *argv[]);
int str_cmp(const char *src, const char *dest, int len);

int main (int argc, const char *argv[])
{
    //args : config range(KB)
    if (argc != 3)
        return 0;
    //function_test(argc, argv);
    return 0;
}

void test(int argc, const char *argv[])
{
    
}

void function_test(int argc, const char *argv[])
{

    //init
    int serverNum = cinquainInitBackStore(argc, argv);
    printf("servernum %d\n", serverNum);
    if (serverNum <= 0)
        return; 

    int range=0, i=0;
    sscanf(argv[2], "%d", &range);
    range *= 1024; //KB

    //buffer & reply handler
    char *buffer = malloc(sizeof(char)*128*M);
    const char **r;

    //gen random key ...
    long rn;
    srand(time(0));
    rn = rand();

    //write range with buffer at specific offset
    cinquainWriteRange((char *)(&rn), sizeof(long), 511*M, buffer, 128*M);
    cinquainGetErr();
    //then read range as the same
    r = (const char **)cinquainReadRange((char *)(&rn), sizeof(long), 511*M, 128*M);
    cinquainGetErr();
    //check correctness
    if (!r || str_cmp(buffer, *r, 128*M))
        printf("!!!write error!!!\n");
    if(r)
        cinquainDeleteBufferHost(r);
    //get length of the key
    printf("strlen %d\n", cinquainStrlen((char *)(&rn), sizeof(long)));
    cinquainGetErr();
    //append with buffer given a invalid current length, so you can use append if you do not make sure the current length
    cinquainAppend((char *)(&rn), sizeof(long), buffer, 768*M, -2);
    free(buffer);
    //get length again
    printf("strlen %d\n", cinquainStrlen((char *)(&rn), sizeof(long)));
    cinquainGetErr();
    //incr & decr key ref
    printf("incr %d\n", cinquainIncrease((char *)(&rn), sizeof(long)));
    printf("incr %d\n", cinquainIncrease((char *)(&rn), sizeof(long)));
    printf("decr %d\n", cinquainDecrease((char *)(&rn), sizeof(long)));
    //remove key 
    cinquainRemove((char *)(&rn), sizeof(long));
    cinquainGetErr();
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
