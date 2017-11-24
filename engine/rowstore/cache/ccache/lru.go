package ccache

// #cgo CFLAGS: -I${SRCDIR}/src
// #cgo LDFLAGS: -L${SRCDIR} -llru_cache -lstdc++ -lc
// #include "stdlib.h"
// #include "c.h"
import "C"
import (
	"math"
	"reflect"
	"unsafe"
)

const maxArrayLen = math.MaxInt32

// CLRUCache  c lru cache
type CLRUCache struct {
	rep *C.lru_cache_t
}

func stringToCSlice(s string) (cs C.lru_slice_t) {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))
	cs.data = (*C.char)(unsafe.Pointer(strHeader.Data))
	cs.len = (C.int)(len(s))
	return
}

// NewCLRUCache new
func NewCLRUCache(capacity int64) *CLRUCache {
	lc, err := C.lru_cache_create(C.int(capacity))
	if err != nil {
		panic(err)
	}

	return &CLRUCache{
		rep: lc,
	}
}

// Put put
func (c *CLRUCache) Put(key string, data []byte) unsafe.Pointer {
	var cdata C.lru_slice_t
	cdata.data = (*C.char)(C.CBytes(data))
	cdata.len = C.int(len(data))
	handle := C.lru_cache_insert(c.rep, stringToCSlice(key), cdata, C.int(len(data)))
	return handle
}

// Get get
func (c *CLRUCache) Get(key string) unsafe.Pointer {
	handle := C.lru_cache_lookup(c.rep, stringToCSlice(key))
	return unsafe.Pointer(handle)
}

func (c *CLRUCache) Value(handle unsafe.Pointer) []byte {
	if handle == nil {
		return nil
	}
	value := C.lru_cache_value(c.rep, handle)
	return (*[maxArrayLen]byte)(unsafe.Pointer(value.data))[:value.len:value.len]
}

func (c *CLRUCache) Release(handle unsafe.Pointer) {
	C.lru_cache_release(c.rep, handle)
}

func (c *CLRUCache) Usage() int {
	return int(C.lru_cache_total_charge(c.rep))
}
