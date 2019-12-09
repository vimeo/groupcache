// Code generated by "stringer -type=CacheType"; DO NOT EDIT.

package galaxycache

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[MainCache-1]
	_ = x[HotCache-2]
	_ = x[CandidateCache-3]
}

const _CacheType_name = "MainCacheHotCacheCandidateCache"

var _CacheType_index = [...]uint8{0, 9, 17, 31}

func (i CacheType) String() string {
	i -= 1
	if i < 0 || i >= CacheType(len(_CacheType_index)-1) {
		return "CacheType(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _CacheType_name[_CacheType_index[i]:_CacheType_index[i+1]]
}
