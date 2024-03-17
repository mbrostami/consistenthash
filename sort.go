package consistenthash

import "sort"

// IntSlice attaches the methods of Interface to []int, sorting in increasing order.
type UIntSlice []uint32

func (x UIntSlice) Len() int           { return len(x) }
func (x UIntSlice) Less(i, j int) bool { return x[i] < x[j] }
func (x UIntSlice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// Sort is a convenience method: x.Sort() calls Sort(x).
func (x UIntSlice) Sort() { sort.Sort(x) }

func SortUInts(x []uint32) {
	sort.Sort(UIntSlice(x))
}
