package cache

type Metrics struct {
	Hits int64

	Misses int64

	Evictions int64

	Expired int64

	CleanupRuns int64

	TotalSets int64

	TotalDeletes int64
}
