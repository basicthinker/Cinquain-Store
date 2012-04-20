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
//  cinquain_store.h
//  Cinquain-Store
//
//  Created by Jinglei Ren <jinglei.ren@gmail.com> on 2/16/12.
//

#ifndef CINQUAIN_STORE_H_
#define CINQUAIN_STORE_H_

typedef unsigned int offset_t;

// Macro Utility.
// Retrieves the address of host structure.
// Parameters include the pointer to the buffer that itself is a pointer,
//    the implementation-specific host structure of the buffer,
//    and the name of the buffer within the host structure defination.
#define cinquainBufferHost(ptr, type, member) \
  ((type *)((char *)(ptr)-(unsigned long)(&((type *)0)->member)))

// Initializes the back store.
// Returns the number of alive instances of back store. 
//    Negative values indicate error.
int cinquainInitBackStore(const int argc, const char *argv[]);

// Reads byte-range data from the value associated with the specified key.
// To avoid unnecessary memory copy, no outside memory buffer is allocated,
// and this function directly returns the address of the inner buffer.
// 
// The address of the inner structure that hosts the buffer 
// can be restored from the buffer address via cinquainBufferHost so that
// the inner host structure can be deleted later outside cinquainReadRange.
//
// A null pointer is returned on error or if the requested length of data
// cannot be fulfilled.
char **cinquainReadRange(const char *key, const int key_length,
                         const offset_t offset, const offset_t length,
                         const offset_t file_size);

// Deconstructor of the host structure of
// the buffer returned by cinquainReadRange. 
//
// In this function, the address of the buffer host
// should be restored via cinquainBufferHost.
int cinquainDeleteBufferHost(const char **value, const offset_t file_size);

// Writes data to a byte range of the value
// associated with the specified key.
//
// Inside the function, the input buffer can be directly used
// since the buffer is guaranteed to be available during the function,
// so that no extra buffer needs to be allocated therein.
//
// This function is synchronous, namely the input data are present
// in back store before the function returns, so that the input buffer
// can be deleted thereafter.
//
// Returns the length of data actually written.
// Normally it should equal to value_length.
int cinquainWriteRange(const char *key, const int key_length,
                       offset_t offset,
                       const char *value, const offset_t value_length,
                       const offset_t file_size);

// Appends data to the value associated with the specified key.
// Since this function is often used continuously, the parameter
// current_length is provided for better performance.
// However, the caller has to guarantee its validity.
// When the current_length is zero, a new pair of key-value is created.
//
// Sample usage to put a new pair of key-value to the back store:
//    offset_t current_length = 0;
//    // Code to fill value buffer
//    while (current_length < total_length) {
//      current_length = cinquainAppend(key, key_length, 
//                                      value, value_length, 
//                                      current_length, file_size);
//      // Code to fill value buffer
//    }
//
offset_t cinquainAppend(const char *key, const int key_length,
                        const char *value, const offset_t value_length,
                        const offset_t current_length,
                        const offset_t file_size);

// Atomically increases the reference count (an integer value)
// associated with the specified key.
// Returns the updated value.
int cinquainIncrease(const char *key, const int key_length);

// Atomically decreases the reference count (an integer value)
// associated with the apecified key.
// Returns the updated value.
int cinquainDecrease(const char *key, const int key_length);

// Removes all values associated with the specified key.
// Multiple values include its reference count, and physical blocks of
// a large logic value.
// return 0 for success & other values fail
int cinquainRemove(const char *key, const int key_length,
                   const offset_t file_size);

//return the length of the string vaule stored at the key.
//return 0 if key does not exist.
int cinquainStrlen(const char *key, const int key_length);

//return the error number.
//return 0 if no errors.
int cinquainGetErr();

//return the memory redis servers used in bytes.
//return error number if has errors.
long long cinquainUsedMemory();

#endif // CINQUAIN_STORE_H_
