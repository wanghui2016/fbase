//
// Created by jonah on 17-4-7.
//

#ifndef SHARKSTORE_ENGINE_LIBLRU_C_H_
#define SHARKSTORE_ENGINE_LIBLRU_C_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lru_slice_t {
    char *data;
    int len;
} lru_slice_t;


typedef struct lru_cache_t lru_cache_t;

// 创建lru缓存
lru_cache_t* lru_cache_create(int capacity);

// 插入, 返回handle
// value: value->data由外部使用malloc分配。当被置换出或释放时内部会调用free释放
// charge：插入的条目需要占用多少容量
void* lru_cache_insert(lru_cache_t* c, lru_slice_t key, lru_slice_t value, int charge);


// 查找，返回handle，用完需要调用release接口释放
void* lru_cache_lookup(lru_cache_t *c, lru_slice_t key);

// 获取handle对应的value
lru_slice_t lru_cache_value(lru_cache_t *c, void* handle);

// 用完释放handle
void lru_cache_release(lru_cache_t *c, void *handle);

// 删除
void lru_cache_erase(lru_cache_t *c, lru_slice_t key);

// 获取lru缓存当前的使用量
int lru_cache_total_charge(lru_cache_t* c);

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif //SHARKSTORE_ENGINE_LIBLRU_C_H_

