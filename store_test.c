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
//  Created by Weichao Guo <guoweichao2010@gmail.com> on 2/16/12.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <unistd.h>

#include "cinquain_store.h"
#include "internal.h"

#define K (1024)
#define M (1024*1024)
#define G (1024*1024*1024)

void function_test();
double read_test(offset_t range, int n);
double write_test(offset_t range, int n);
int str_cmp(const char *src, const char *dest, int len);
long long fill_data();
int bSearch(int *a, int n, int key);
void isort(double *a, int n);
void swap(double *a, double *b);
int fill_buffer(char *filename, char *buffer, int size);

//file size(2^i bytes) histogram with total 10000  
long long filesizeHist[28] = {50, 20, 15, 10, 100, 100, 150, 200, 400, 600, 1100, 1250, 1100, 1000, 900, 800, 700, 500, 300, 250, 150, 100, 75, 60, 25, 20, 15, 10};
long long total = 3;
int unit = 10000;
int pi = 28;
int b[29] = {0};
int tunit = 8;

int main (int argc, const char *argv[])
{
    //args : config range(KB) n
    if (argc != 4)
        return 0;
    //init
    int serverNum = cinquainInitBackStore(argc, argv);
    //printf("servernum %d\n", serverNum);
    if (serverNum <= 0)
        return; 

    int i, n;
    double r[16], w[16];
    for (i=1; i<pi+1; i++)
        b[i] = b[i-1] + filesizeHist[i-1] * total;

    offset_t range=0;
    sscanf(argv[2], "%u", &range);
    sscanf(argv[3], "%d", &n);
    range *= 1024; //KB

    //printf("%lld\n", cinquainUsedMemoryRss());
    //fill_data();

    //printf("%lf\n", read_test(range, 1000)/M);
    printf("%lf\n", write_test(range, 100)/M);
    // test ...
    /* 
    for (i=0 ; i<n; i++) {
        r[i] = read_test(range, 10) / M;
        sleep(5);
    }
    for (i=0 ; i<n; i++) {
        w[i] = write_test(range, 10) / M;
        sleep(5);
    }
    isort(r, n);
    isort(w, n);
    for (i=0; i<n; i++)
        printf("%lf\t", r[i]);
    printf("|\t");
    for (i=0; i<n; i++)
        printf("%lf\t", w[i]);
    printf("\n");
    
*/
    //function_test();

    return 0;
}

long long fill_data()
{
    long long key = 0;
    long long storage = 0, hasWritten = 0, t = 1, usedMemory, temp=0;
    int i, j, n, times, bufferSize = 128*M;
    char *buffer = malloc(sizeof(char)*bufferSize);
    if (fill_buffer("largefile", buffer, bufferSize))
        return -1;
    for (i=0; i<pi; i++){
        n = total*filesizeHist[i];
        for (j=0; j<n; j++) {
            hasWritten += cinquainWriteRange((char *)(&key), sizeof(key), 0, buffer, 1<<i, 1<<i);
            cinquainGetErr();
            usleep((1<<i)/tunit);
            /*
            if (hasWritten > t*128*M) {
                times = 0;
                sleep(5);
                temp = usedMemory = cinquainUsedMemory();
                while (usedMemory > 512*M && times < 15) {
                    sleep(5);
                    usedMemory = cinquainUsedMemory();
                    printf("Used Memory : %lld Redis Swap Speed : %lld\n", usedMemory, (usedMemory - temp)/M);
                    temp = usedMemory;
                    times++;
                }
                t++;
            }
            */
            
            key++;
        }
        storage += total*filesizeHist[i]*(1<<i);
    }

    free(buffer);

    printf("fill data size : %lld bytes -- %lf G\nreal written : %lld\nkeys : %lld\n", storage, (double)storage/G, hasWritten, key);
    return hasWritten == storage;
}

double read_test(offset_t range, int n)
{
    int i;
    long long key = 0, hasRead = 0, ftime = 0;
    offset_t offset, size;
    const char **r;
    struct timeval start, end, fstart, fend;
    srand(time(0));

    gettimeofday(&start, NULL);
    //read n random keys ...
    for (i=0; i<n; i++) {
        offset = 0;
        key = rand()*total*unit/RAND_MAX;
        size = 1<<bSearch(b, pi, key);
        //printf("%lld %d\n", key, size);
        while (offset < size) {
            gettimeofday(&fstart, NULL);
            r = (const char **)cinquainReadRange((char *)(&key), sizeof(key), offset, range, size);
            gettimeofday(&fend, NULL);
            ftime += ((fend.tv_sec-fstart.tv_sec)*1000000 + fend.tv_usec - fstart.tv_usec);
            //usleep(range/tunit);
            
            if (r) {
                hasRead += range;
                cinquainDeleteBufferHost(r, size);
            }
            offset += range;
        }
    }

    gettimeofday(&end, NULL);

    //return hasRead*1000000/((end.tv_sec-start.tv_sec)*1000000 + end.tv_usec - start.tv_usec);
    return hasRead*1000000/ftime;
}

double write_test(offset_t range, int n)
{
    int i , t = 1;
    long long key = 0, hasWritten = 0, ftime = 0;
    offset_t offset, size, count;
    char *cur;
    struct timeval start, end, fstart, fend;
    
    int bufferSize = 1*M;
    //char buffer[256*K];
    char *buffer = malloc(sizeof(char)*bufferSize);
    if (fill_buffer("largefile", buffer, bufferSize) != 0)
        return -1;
    srand(time(0));

    gettimeofday(&start, NULL);

    //write ...
    for (i=0; i<n; i++) {
        offset = 0;
        key = rand()*total*unit/RAND_MAX;
        size = 1<<bSearch(b, pi, key);
        //printf("%lld %d\n", key, size);
        while (offset < size) {
            count = size - offset > range ? range : size - offset;
            gettimeofday(&fstart, NULL);
            hasWritten += cinquainWriteRange((char *)(&key), sizeof(key), offset, buffer+(offset%bufferSize), count, size);
            gettimeofday(&fend, NULL);
            usleep(range/tunit);
            ftime += ((fend.tv_sec-fstart.tv_sec)*1000000 + fend.tv_usec - fstart.tv_usec);
            //cinquainGetErr();
            offset += count;
            //cur += count;
        }
    }

    gettimeofday(&end, NULL);

    free(buffer);

    //return hasWritten*1000000/((end.tv_sec-start.tv_sec)*1000000 + end.tv_usec - start.tv_usec);
    return hasWritten*1000000/ftime;
}
void function_test()
{

    //buffer & reply handler
    char *buffer = malloc(sizeof(char)*128*M);
    const char **r;

    //gen random key ...
    long rn;
    srand(time(0));
    rn = rand();

    //write range with buffer at specific offset
    cinquainWriteRange((char *)(&rn), sizeof(long), 0, buffer, 128*M, 128*M);
    cinquainGetErr();
    //then read range as the same
    r = (const char **)cinquainReadRange((char *)(&rn), sizeof(long), 0, 128*M, 128*M);
    cinquainGetErr();
    //check correctness
    if (!r || str_cmp(buffer, *r, 128*M))
        printf("!!!write error!!!\n");
    if(r)
        cinquainDeleteBufferHost(r, 128*M);
    //get length of the key
    printf("strlen %d\n", cinquainStrlen((char *)(&rn), sizeof(long)));
    cinquainGetErr();
    //append with buffer given a invalid current length, so you can use append if you do not make sure the current length
    cinquainAppend((char *)(&rn), sizeof(long), buffer, 128*M, 128*M, 128*M);
    free(buffer);
    //get length again
    printf("strlen %d\n", cinquainStrlen((char *)(&rn), sizeof(long)));
    cinquainGetErr();
    //incr & decr key ref
    printf("incr %d\n", cinquainIncrease((char *)(&rn), sizeof(long)));
    printf("incr %d\n", cinquainIncrease((char *)(&rn), sizeof(long)));
    printf("decr %d\n", cinquainDecrease((char *)(&rn), sizeof(long)));
    //remove key 
    cinquainRemove((char *)(&rn), sizeof(long), 128*M);
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

int bSearch(int *a, int n, int key) {
    int l=0, r=n, m;
    while (l + 1 < r) {
        m = (l + r)/2;
        if (key < a[m])
            r = m;
        else
            l = m;
    }
    return l;
}

void isort(double *a, int n) {
    int i, j;
    for (i=1; i<n; i++)
        for (j=i; j>0; j--)
            if (a[j] < a[j-1])
                swap(&a[j], &a[j-1]);
}

void swap(double *a, double *b) {
    double t = *a;
    *a = *b;
    *b = t;
    return ;
}

int fill_buffer(char *filename, char *buffer, int size)
{
    FILE *fp = fopen(filename, "rb");
    if(fp) {
        fread(buffer, sizeof(char), size, fp);
        fclose(fp);
        return 0;
    }
    else {
        free(buffer);
        return -1;
    }
}
