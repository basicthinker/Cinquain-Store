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

#define K (1024)
#define M (1024*1024)
#define G (1024*1024*1024)

void function_test();
double read_test(offset_t range, int n);
double write_test(offset_t range, char *buffer, int n);
int str_cmp(const char *src, const char *dest, int len);
long long fill_data(char *buffer);
int bSearch(int *a, int n, int key);
void isort(double *a, int n);
void swap(double *a, double *b);

//file size(2^i bytes) histogram with total 10000  
long long filesizeHist[28] = {50, 20, 15, 10, 100, 100, 150, 200, 400, 600, 1100, 1250, 1100, 1000, 900, 800, 700, 500, 300, 250, 150, 100, 75, 60, 25, 20, 15, 10};
long long total = 1; //unit : 10000
int b[29] = {0};

int main (int argc, const char *argv[])
{
    //args : config range(KB) n
    if (argc != 4)
        return 0;
    //init
    int serverNum = cinquainInitBackStore(argc, argv);
    printf("servernum %d\n", serverNum);
    if (serverNum <= 0)
        return; 

    int i, n;
    double rtime[16], wtime[16];
    for (i=1; i<29; i++)
        b[i] = b[i-1] + filesizeHist[i-1] * total;

    offset_t range=0;
    sscanf(argv[2], "%u", &range);
    sscanf(argv[3], "%d", &n);
    range *= 1024; //KB

    FILE *fp = fopen("largefile", "rb");
    char *buffer = malloc(sizeof(char)*128*M);
    if(fp) {
        fread(buffer, sizeof(char), 128*M, fp);
        fclose(fp);
    }

    fill_data(buffer);

    // test ...
    /*
    for (i=0 ; i<n; i++)
        rtime[i] = read_test(range, 10000);
    for (i=0 ; i<n; i++)
        wtime[i] = write_test(range, buffer, 10000);
    isort(rtime, n);
    isort(wtime, n);
    for (i=0; i<n; i++)
        printf("%lf\t", rtime[i]);
    printf("|\t");
    for (i=0; i<n; i++)
        printf("%lf\t", wtime[i]);
    printf("\n");
    */

    //function_test();

    free(buffer);

    return 0;
}

long long fill_data(char *buffer)
{
    long long key = total*10000 - 1;
    long long storage = 0, hasWritten = 0;
    int i, j, n;
    for (i=27; i>=0; i--){
        n = total*filesizeHist[i];
        for (j=0; j<n; j++) {
            hasWritten += cinquainWriteRange((char *)(&key), sizeof(key), 0, buffer, 1<<i);
            while (cinquainGetErr()) {
                sleep(10);
                printf("%lld\n", key);
                hasWritten += cinquainWriteRange((char *)(&key), sizeof(key), 0, buffer, 1<<i);
            }
            key--;
        }
        storage += total*filesizeHist[i]*(1<<i);
    }
    printf("fill data size : %lld bytes -- %lf G\nreal written : %lld\nkeys : %lld\n", storage, (double)storage/G, hasWritten, key);
    return hasWritten == storage;
}

double read_test(offset_t range, int n)
{
    int i;
    long long key = 0;
    offset_t offset, size;
    const char **r;
    struct timeval start, end;
    srand(time(0));

    gettimeofday(&start, NULL);
    //read n random keys ...
    for (i=0; i<n; i++) {
        offset = 0;
        key = (double)rand()*total*10000/RAND_MAX;
        size = 1<<bSearch(b, 28, key);
        while (offset < size) {
            //r = (const char **)cinquainReadRange((char *)(&key), sizeof(key), offset, range);
            //cinquainDeleteBufferHost(r);
            offset += range;
        }
    }

    gettimeofday(&end, NULL);

    return ((end.tv_sec-start.tv_sec)*1000000 + end.tv_usec - start.tv_usec)/1000000;
}

double write_test(offset_t range, char *buffer, int n)
{
    int i;
    long long key = 0;
    offset_t offset, size, count;
    char *cur;
    struct timeval start, end;
    srand(time(0));

    gettimeofday(&start, NULL);

    //write ...
    for (i=0; i<n; i++) {
        offset = 0;
        key = (double)rand()*total*10000/RAND_MAX;
        size = 1<<bSearch(b, 28, key);
        while (offset < size) {
            count = size - offset > range ? range : size - offset;
            //cinquainWriteRange((char *)(&key), sizeof(key), offset, cur, count);
            offset += count;
            cur += count;
        }
    }

    gettimeofday(&end, NULL);

    return ((end.tv_sec-start.tv_sec)*1000000 + end.tv_usec - start.tv_usec)/1000000;
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
    cinquainAppend((char *)(&rn), sizeof(long), buffer, 128*M, -2);
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
