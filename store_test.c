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

#include "cinquain_store.h"

int main (int argc, const char *argv[])
{
    int serverNum = cinquainInitBackStore(argc, argv);
    char key[21] = {0xda,0x39,0xa3,0xee,0x5e,0x6b,0x4b,0x0d,0x32,0x55,0xbf,0xef,0x95,0x60,0x18,0x90,0xaf,0xd8,0x07,0x09};
    char *value = "testtesttesttesttesttest";
    const char **readReply;
    if (serverNum > 1) {
        cinquainAppendRange(key, 20, 0, value, 20);
        readReply = (const char **)cinquainReadRange(key, 20, 2, 3);
        cinquainDeleteBufferHost(readReply);
    }
    return 0;
}
