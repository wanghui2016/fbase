//
// Created by jonah on 17-4-7.
//
#include <iostream>
#include "cache.h"
#include "c.h"

using sharkstore::Cache;
using sharkstore::Slice;

extern "C" {

struct lru_cache_t {
    Cache* rep_;
};

lru_cache_t* lru_cache_create(int capacity) {
    Cache* c = sharkstore::NewLRUCache(capacity);
    if (NULL == c) {
        return NULL;
    }
    lru_cache_t* result = new lru_cache_t;
    result->rep_ = c;
    return result;

}

static void valueDeleter(const Slice& key, void *value) {
    if (value != NULL ) {
        auto sv = reinterpret_cast<lru_slice_t*>(value);
        free(sv->data);
        delete(sv);
    }
}

void* lru_cache_insert(lru_cache_t* c, lru_slice_t key, lru_slice_t value, int charge) {
    assert(c);
    auto svalue = new lru_slice_t;
    svalue->data = value.data;
    svalue->len = value.len;
    auto skey = Slice(key.data, key.len);
    return c->rep_->Insert(skey, svalue, charge, valueDeleter);
}

void* lru_cache_lookup(lru_cache_t *c, lru_slice_t key) {
    assert(c != NULL);
    auto h = c->rep_->Lookup(Slice(key.data, key.len));
    if (NULL == h) {
        return NULL;
    }
    return h;
}

lru_slice_t lru_cache_value(lru_cache_t* c, void * h) {
    assert(c != NULL);
    auto sv = c->rep_->Value(reinterpret_cast<Cache::Handle*>(h));
    if (sv != NULL) {
        return *(reinterpret_cast<lru_slice_t*>(sv));
    } else {
        errno = ENODATA;
        lru_slice_t result;
        result.data = NULL;
        result.len = 0;
        return result;
    }
}

void lru_cache_erase(lru_cache_t *c, lru_slice_t key) {
    assert(c != NULL);
    c->rep_->Erase(Slice(key.data, key.len));
}

void lru_cache_release(lru_cache_t *c, void* h) {
    assert(c != NULL);
    c->rep_->Release(reinterpret_cast<Cache::Handle*>(h));
}

int lru_cache_total_charge(lru_cache_t* c) {
    assert(c != NULL);
    return c->rep_->TotalCharge();
}

}
