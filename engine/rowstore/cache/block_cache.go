package cache
//
//import "engine/rowstore/cache/ccache"
//import "fmt"
//
//// SetBlockFunc set block data func
//type SetBlockFunc func() []byte
//
//// BlockCacher the block cacher
//type BlockCacher interface {
//	Get(rng, ns, key uint64, setFunc SetBlockFunc) *Handle
//	Block(h *Handle) []byte
//	Release(h *Handle)
//
//	Capacity() int64
//	Usage() int64
//	Close()
//}
//
//// BlockCacheType block cache type
//type BlockCacheType int
//
//const (
//	// BCacheTypeNative native(golang)
//	BCacheTypeNative BlockCacheType = iota
//	// BCacheTypeCGo  use cgo version
//	BCacheTypeCGo
//)
//
//// NewBlockCache create new block cacher
//func NewBlockCache(typ BlockCacheType, capacity int64) BlockCacher {
//	switch typ {
//	case BCacheTypeNative:
//		return newNativeBlockCache(capacity)
//	case BCacheTypeCGo:
//		return newCBlockCache(capacity)
//	default:
//		return nil
//	}
//}
//
//type nativeBlockCache struct {
//	rep *Cache
//}
//
//func newNativeBlockCache(capacity int64) *nativeBlockCache {
//	return &nativeBlockCache{
//		rep: NewCache(NewLRU(capacity)),
//	}
//}
//
//func (c *nativeBlockCache) Get(rng, ns, key uint64, f SetBlockFunc) *Handle {
//	setFunc := func() (size int64, value Value) {
//		data := f()
//		size = int64(len(data))
//		value = data
//		return
//	}
//	return c.rep.Get(rng, ns, key, setFunc)
//}
//
//func (c *nativeBlockCache) Block(h *Handle) []byte {
//	value := h.Value()
//	if value != nil {
//		return h.Value().([]byte)
//	}
//	return nil
//}
//
//func (c *nativeBlockCache) Release(h *Handle) {
//	if h != nil {
//		h.Release()
//	}
//}
//
//func (c *nativeBlockCache) Capacity() int64 {
//	return c.rep.Capacity()
//}
//
//func (c *nativeBlockCache) Usage() int64 {
//	return c.rep.Size()
//}
//
//func (c *nativeBlockCache) Close() {
//	if c.rep != nil {
//		c.rep.Close()
//		c.rep = nil
//	}
//}
//
//type cBlockCache struct {
//	rep *ccache.CLRUCache
//}
//
//func newCBlockCache(capacity int64) *cBlockCache {
//	return &cBlockCache{
//		rep: ccache.NewCLRUCache(capacity),
//	}
//}
//
//func (c *cBlockCache) Get(rng, ns, key uint64, f SetBlockFunc) *Handle {
//	k := fmt.Sprintf("%d/%d/%d", rng, ns, key)
//	p := c.rep.Get(k)
//	if p == nil {
//		data := f()
//		p = c.rep.Put(k, data)
//	}
//	return &Handle{n: p}
//}
//
//func (c *cBlockCache) Block(h *Handle) []byte {
//	if h == nil {
//		return nil
//	}
//	return c.rep.Value(h.n)
//}
//
//func (c *cBlockCache) Release(h *Handle) {
//	if h != nil {
//		c.rep.Release(h.n)
//	}
//}
//
//func (c *cBlockCache) Capacity() int64 {
//	return 0
//}
//
//func (c *cBlockCache) Usage() int64 {
//	return 0
//}
//
//func (c *cBlockCache) Close() {
//}
