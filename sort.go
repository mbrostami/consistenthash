package consistenthash

import "sort"

// UIntSlice attaches the methods of Interface to []uint32, sorting in increasing order.
type UIntSlice []uint32

func (x UIntSlice) Len() int           { return len(x) }
func (x UIntSlice) Less(i, j int) bool { return x[i] < x[j] }
func (x UIntSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func SortUints(x []uint32) {
	sort.Sort(UIntSlice(x))
}
