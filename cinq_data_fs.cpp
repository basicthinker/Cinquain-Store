#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <sys/stat.h>

#include "cinquain_store.h"

#include <iostream>
#include <string.h>
#include <string>
#include <unordered_map>
#include <pthread.h>

using namespace std;

#define NUM_BUCKETS 1000

// TODO Metadata
static unordered_map<string, struct stat> gKeyMeta(NUM_BUCKETS);
static pthread_rwlock_t gMetaLock = PTHREAD_RWLOCK_INITIALIZER;

// Set value or create a new key if it does not exist.
static inline void SetKeyMeta(const char *ckey, const struct stat* meta) {
  string skey(ckey);
  pthread_rwlock_wrlock(&gMetaLock);
  gKeyMeta[skey] = *meta;
  pthread_rwlock_unlock(&gMetaLock);
}

// Returns NULL if the key does not exist.
static inline struct stat *GetKeyMeta(const char *ckey) {
  string skey(ckey);
  pthread_rwlock_rdlock(&gMetaLock);
  unordered_map<string, struct stat>::iterator it = gKeyMeta.find(skey);
  struct stat *meta = it == gKeyMeta.end() ? NULL : &it->second;
  pthread_rwlock_unlock(&gMetaLock);
  return meta;
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
  const char *key = PathPick(path);
  struct stat *meta = GetKeyMeta(key);
  if (!meta) return -ENOENT;
  memcpy(attr, meta, sizeof(struct stat));
  return 0;
}

int cinqReadDir(const char *path, void *buf, fuse_fill_dir_t filldir, off_t offset, struct fuse_file_info *finfo) {
  // path is ignored
  pthread_rwlock_rdlock(&gMetaLock);
  for (unordered_map<string, struct stat>::iterator it = gKeyMeta.begin(); it != gKeyMeta.end(); ++it) {
    filldir(buf, it->first.c_str(), NULL, 0);
  }
  pthread_rwlock_unlock(&gMetaLock);
  return 0;
}

int cinqChMod(const char *path, mode_t mode) {
  const char *key = PathPick(path);
  struct stat *attr = GetKeyMeta(key);
  if (!attr) return -ENOENT;
  attr->st_mode = mode;
  return 0;
}

int cinqChOwn(const char *path, uid_t uid, gid_t gid) {
  const char *key = PathPick(path);
  struct stat *attr = GetKeyMeta(key);
  if (!attr) return -ENOENT;
  attr->st_uid = uid;
  attr->st_gid = gid;
  return 0;
}

int cinqTruncate(const char *path, off_t size) {
  const char *key = PathPick(path);
  struct stat *attr = GetKeyMeta(key);
  if (!attr) return -ENOENT;
  attr->st_size = size;
  return 0;
}

int cinqCreate(const char *path, mode_t mode, struct fuse_file_info *finfo) {
  const char *key = PathPick(path);
  struct stat attr;
  memset(&attr, 0, sizeof(struct stat));
  attr.st_mode = mode;
  attr.st_nlink = 1;
  SetKeyMeta(key, &attr);
  return 0;
}

int cinqRead(const char *path, char *buf, size_t len, off_t offset, struct fuse_file_info *finfo) {
  const char *key = PathPick(path);
  struct stat *meta = GetKeyMeta(key);
  if (!meta) return -ENOENT;

  if (meta->st_size < offset + (off_t)len) len = meta->st_size - offset;
  if (len < 0) return -EINVAL;

  char **data = cinquainReadRange(key, strlen(key), offset, len, 0);
  if (!data) return -EIO;
  memcpy(buf, *data, len);
  cinquainDeleteBufferHost((const char **)data, 0);
  return 0;
}

int cinqWrite(const char *path, const char *buf, size_t len, off_t offset, struct fuse_file_info *finfo) {
  const char *key = PathPick(path);
  struct stat *meta = GetKeyMeta(key);
  if (!meta) return -ENOENT;

  if (offset + (off_t)len > meta->st_size) {
    meta->st_size = offset + len;
  }

  size_t written = 0;
  while (written < len) {
    written += cinquainWriteRange(key, strlen(key),
        offset + written, buf + written, len - written, 0);
  } // TODO Cancel on exeption
  return 0;
}

int main(int argc, char *argv[]) {
  struct fuse_operations cinqOperations;
  cinqOperations.getattr = cinqGetAttr;
  cinqOperations.chmod = cinqChMod;
  cinqOperations.chown = cinqChOwn;
  cinqOperations.truncate = cinqTruncate;
  cinqOperations.readdir = cinqReadDir;
  cinqOperations.create = cinqCreate;
  cinqOperations.read = cinqRead;
  cinqOperations.write = cinqWrite;

  int ret = fuse_main(argc, argv, &cinqOperations, NULL);
  cout << "FUSE returned " << ret << "." << endl;
  return ret;
}
