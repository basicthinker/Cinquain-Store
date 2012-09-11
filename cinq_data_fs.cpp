#include <fuse.h>
#include <errno.h>
#include <sys/stat.h>

#include "cinquain_store.h"

#include <string.h>
#include <string>
#include <unordered_map>
#include <pthread.h>

using namespace std;

#define NUM_BUCKETS 1000

// TODO Metadata
static unordered_map<string, struct stat> gKeyMeta(NUM_BUCKETS);
static pthread_rwlock_t gMetaLock = PTHREAD_RWLOCK_INITIALIZER;

static inline void SetKeyMeta(const char *ckey, const struct stat* meta) {
  string skey(ckey);
  pthread_rwlock_wrlock(&gMetaLock);
  gKeyMeta[skey] = *meta;
  pthread_rwlock_unlock(&gMetaLock);
}

static inline void GetKeyMeta(const char *ckey, struct stat* meta) {
  string skey(ckey);
  pthread_rwlock_rdlock(&gMetaLock);
  *meta = gKeyMeta[skey];
  pthread_rwlock_unlock(&gMetaLock);
}

static char *PathPick(const char *path) {
  const int len = strlen(path);
  if (len == 0) return NULL;

  char *last = (char *)(path + len - 1);
  while (last > path && *last != '/') {
    --last;
  }
  
  return *last == '/' ? last + 1 : last;
}

int cinqGetAttr(const char *path, struct stat *attr) {
  char *ckey = PathPick(path);
  GetKeyMeta(ckey, attr);
  return 0;
}



