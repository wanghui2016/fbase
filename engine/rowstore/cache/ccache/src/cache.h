//
// Created by jonah on 17-4-7.
//

#ifndef SHARKSTORE_ENGINE_LRU_CACHE_CAHCE_H_
#define SHARKSTORE_ENGINE_LRU_CACHE_CAHCE_H_

#include "slice.h"

namespace sharkstore {

class Cache;

extern Cache* NewLRUCache(size_t capacity);

class Cache {
public:
    Cache() { }

    virtual ~Cache();

    struct Handle { };

    virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                           void (*deleter)(const Slice& key, void* value)) = 0;

    virtual Handle* Lookup(const Slice& key) = 0;

    virtual void Release(Handle* handle) = 0;

    virtual void* Value(Handle* handle) = 0;

    virtual void Erase(const Slice& key) = 0;

    virtual uint64_t NewId() = 0;

    virtual void Prune() {}

    virtual size_t TotalCharge() const = 0;

private:
    void LRU_Remove(Handle* e);
    void LRU_Append(Handle* e);
    void Unref(Handle* e);

    struct Rep;
    Rep* rep_;

    Cache(const Cache&) = delete;
    void operator=(const Cache&) = delete;
};
}

#endif //SHARKSTORE_ENGINE_LRU_CACHE_CAHCE_H_

