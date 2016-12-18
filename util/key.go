package util

const (
	MaxKeySize = 1 << 11
)

var (
	MinKey = []byte{0x00}
	MaxKey []byte
)

func init() {
	MaxKey = make([]byte, MaxKeySize, MaxKeySize)
	for i, len := 0, len(MaxKey); i < len; i++ {
		MaxKey[i] = 0xff
	}
}

// NextKey calculates the closest next key immediately following the given key.
func NextKey(key []byte) []byte {
	if key == nil || len(key) == 0 {
		return nil
	}
	var nextKey []byte
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] < 0xff {
			nextKey = make([]byte, i+1)
			copy(nextKey, key)
			nextKey[i]++
			break
		}
	}
	return nextKey
}

// PrevKey calculates the closest previous key immediately coming the given key.
func PrevKey(key []byte) []byte {
	if key == nil || len(key) == 0 {
		return nil
	}
	var prevKey []byte
	l := len(key)
	if key[l-1] > 0x00 {
		prevKey = make([]byte, l)
		copy(prevKey, key)
		prevKey[l-1]--
	} else {
		prevKey = make([]byte, l-1)
		copy(prevKey, key)
	}
	return prevKey
}
