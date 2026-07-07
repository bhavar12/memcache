package cache

import "time"

type Config struct {
	Capacity        int
	CleanupInterval time.Duration
}
