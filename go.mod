module github.com/mbrostami/consistenthash/v2

go 1.18

// Block-partitioning releases: lookups returned the wrong node once the ring
// spanned more than one block, and they were not concurrency-safe. Use v2.2.0+.
retract (
	v2.0.0
	v2.1.0
)
