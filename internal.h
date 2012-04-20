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
//  internal.h
//  Cinquain-Store
//
//  Created by Jinglei Ren <jinglei.ren@gmail.com> on 4/15/12.
//  Modified by Weichao Guo <guoweichao2010@gmail.com>.
//

#ifndef CINQUAIN_STORE_INTERNAL_H_
#define CINQUAIN_STORE_INTERNAL_H_

// error flags 
#define CINQUAIN_ERR_CONFIG -6
#define CINQUAIN_ERR_CONNECTION -7
#define CINQUAIN_ERR_RANGE -8
#define CINQUAIN_ERR_NX -9
#define CINQUAIN_ERR_REPLY -10

typedef struct {
  unsigned int id;
  offset_t offset;
  offset_t length;
  char * buffer;
} workBlock;

typedef struct {
  workBlock *wb;
  unsigned int blocks;
} workBlocks;

#endif // CINQUAIN_STORE_INTERNAL_H_
