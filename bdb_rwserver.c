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
//  bdb_rwserver.c
//  Cinquain-Store
//  a simple read / write items BDB server
//  Created by Weichao Guo <guoweichao2010@gmail.com> on 9/6/12.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <unistd.h>

#include "cinquain_store.h"
#include "internal.h"

#define BUFSIZE 1024
int main (int argc, const char *argv[]) {

	char buf[BUFSIZE], *first, *value;
    char *readBuffer;
    int hasWritten;

    if (cinquainInitBackStore(argc, argv))
        return -1;

	for (;;) {
		printf("RWSERVER> "),
		fflush(stdout);

		if (fgets(buf, sizeof(buf), stdin) == NULL)
			break;

#define	DELIM " \t\n"
		if ((first = strtok(&buf[0], DELIM)) == NULL) {
			/* Blank input line. */
			value = NULL;
		} else if ((value = strtok(NULL, DELIM)) == NULL) {
			/* Just one input token. */
			if (strncmp(buf, "exit", 4) == 0 ||
			    strncmp(buf, "quit", 4) == 0) {
				break;
			}
            else {
                readBuffer = cinquainReadRange(first, strlen(first), 0, BUFSIZE, BUFSIZE);
                printf("get item : %s %s\n", first, readBuffer);
                if (readBuffer)
                    free(readBuffer);
            }
			continue;
		} else {
			/* Normal two-token input line. */
            hasWritten = cinquainWriteRange(first, strlen(first), 0, value, strlen(value), strlen(value));
            printf("set item : %s %s %d bytes written\n", first, value, hasWritten);
			continue;
			}
		}
}
