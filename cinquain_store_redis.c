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

#include "cinquain_store.h"

int cinquainInitBackStore(const int argc, const char *argv[]) {
  return 0;
}

char **cinquainReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length) {
  return 0;
}

int cinquainDeleteBufferHost(const char **value) {
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


