//
// Created by jonah on 17-4-7.
//

#include <assert.h>
#include <iostream>
#include <string>
#include <cstring>
#include "c.h"

static void deleter(const char *key, void *value) {
    delete reinterpret_cast<std::string*>(value);
}

int main() {
    auto c = lru_cache_create(100);
    assert(c != NULL);

    lru_slice_t value;
    value.data = (char *)malloc(6);
    value.len = 6;
    memcpy(value.data, "Hello", 6);

    const char *ckey = "a";
    lru_slice_t key;
    key.data = (char *)ckey;
    key.len = 1;
    auto h1 = lru_cache_insert(c, key, value, 30);
    assert(h1 != NULL);
    lru_cache_release(c, h1);

    auto h = lru_cache_lookup(c, key);
    assert(h != NULL);
    auto v = lru_cache_value(c, h);
    std::cout << (char*)v.data << std::endl;
}
