package hyperbloom

import (
	XXHN "github.com/OneOfOne/xxhash"
)

func hashEntry(entry []byte, n int) []uint64 {
	/*
	 * Hash an entry "n" number of times with a 64 bit hash
	 */
	out := make([]uint64, n)
	for i := 0; i < n; i++ {
		pert := entry
		pert[len(pert)-1] = (pert[len(pert)-1] & 0xFF) //x Mod y = x & (y-1) if y is power of 2
		out[i] = XXHN.Checksum64(entry)
	}
	return out
}
