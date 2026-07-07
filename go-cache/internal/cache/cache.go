package cache

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type entry[T any] struct {
	key   string
	value Item[T]
}

type Item[T any] struct {
	Value     T
	ExpiresAt time.Time
}

type Cache[T any] struct {
	mu sync.RWMutex

	data map[string]*list.Element

	lru *list.List

	capacity int

	ctx context.Context

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	metrics Metrics
}

type Stats struct {
	Hits int64

	Misses int64

	HitRatio float64

	Evictions int64

	Expired int64

	CleanupRuns int64

	TotalSets int64

	Deletes int64

	Entries int
}

func New[T any](cfg Config) *Cache[T] {

	c := &Cache[T]{
		data:     make(map[string]*list.Element),
		lru:      list.New(),
		capacity: cfg.Capacity,
	}
	ctx, cancel := context.WithCancel(context.Background())

	c.ctx = ctx
	c.cancel = cancel
	c.startCleanup(cfg.CleanupInterval)

	return c
}

func (c *Cache[T]) Set(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// already exist then move to front
	if element, ok := c.data[key]; ok {
		c.lru.MoveToFront(element)
		return
	}
	node := &entry[T]{
		key: key,
		value: Item[T]{
			Value: value,
		},
	}
	// not exit push
	element := c.lru.PushFront(node)
	c.data[key] = element
	// capacity check if it's full or not
	// if it's full then remove lru i.e tail node
	if c.lru.Len() > c.capacity {
		tail := c.lru.Back()
		old := tail.Value.(*entry[T])
		delete(c.data, old.key)
		c.lru.Remove(tail)
	}

	// Looup
	if element, ok := c.data[key]; ok {
		c.lru.MoveToFront(element)
	}

}

func (c *Cache[T]) removeExpired() {

	c.mu.Lock()

	defer c.mu.Unlock()

	now := time.Now()

	for key, element := range c.data {

		item := element.Value.(*entry[T])

		if !item.value.ExpiresAt.IsZero() &&
			now.After(item.value.ExpiresAt) {

			c.lru.Remove(element)

			delete(c.data, key)
		}
	}
}

func (c *Cache[T]) Close() {

	c.cancel()

	c.wg.Wait()
}

func (c *Cache[T]) SetWithttl(key string, value T, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// already exist then move to front
	if element, ok := c.data[key]; ok {
		c.lru.MoveToFront(element)
		return
	}
	node := &entry[T]{
		key: key,
		value: Item[T]{
			Value: value,
		},
	}
	// not exit push
	element := c.lru.PushFront(node)
	c.data[key] = element
	// capacity check if it's full or not
	// if it's full then remove lru i.e tail node
	if c.lru.Len() > c.capacity {
		tail := c.lru.Back()
		old := tail.Value.(*entry[T])
		delete(c.data, old.key)
		c.lru.Remove(tail)
	}

	// Looup
	if element, ok := c.data[key]; ok {
		c.lru.MoveToFront(element)
	}
}

func (item Item[T]) isExpired() bool {
	if item.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(item.ExpiresAt)
}

/* func (c *Cache[T]) Remainingttl(key string) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.data[key]
	if ok {
		if !item.isExpired() {
			return time.Since(item.ExpiresAt).Abs().Round(time.Millisecond)
		}
	}
	return time.Hour
}

func (c *Cache[T]) Touch(key string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item := c.data[key]
	item.ExpiresAt = time.Now().Add(ttl)
	c.data[key] = item
} */

/* func (c *Cache[T]) Get(key string) (T, bool) {
	c.mu.RLock()
	value, ok := c.data[key]
	c.mu.RUnlock()
	var zero T
	if !ok {
		return zero, false
	}
	if value.isExpired() {
		c.mu.Lock()
		delete(c.data, key)
		defer c.mu.Unlock()
		return zero, false
	}
	return value.Value, ok
} */

func (c *Cache[T]) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.data[key]
	c.lru.Remove(element)
	delete(c.data, key)
}

func (c *Cache[T]) Exist(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.data[key]
	return ok
}

func (c *Cache[T]) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.data)
}

func (c *Cache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[string]*list.Element)
}

func (c *Cache[T]) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, len(c.data))
	for key, _ := range c.data {
		keys = append(keys, key)
	}
	return keys
}

/* func (c *Cache[T]) Values() []Item[T] {
	c.mu.Lock()
	defer c.mu.Unlock()
	values := make([]Item[T], len(c.data))
	for _, value := range c.data {
		values = append(values, value.(*entry[T]))
	}
	return values
} */

// return the stats

func (c *Cache[T]) Stats() Stats {

	hits := atomic.LoadInt64(&c.metrics.Hits)

	misses := atomic.LoadInt64(&c.metrics.Misses)

	total := hits + misses

	ratio := 0.0

	if total > 0 {

		ratio = float64(hits) /
			float64(total)
	}

	return Stats{

		Hits: hits,

		Misses: misses,

		HitRatio: ratio,

		Evictions: atomic.LoadInt64(
			&c.metrics.Evictions),

		Expired: atomic.LoadInt64(
			&c.metrics.Expired),

		CleanupRuns: atomic.LoadInt64(
			&c.metrics.CleanupRuns),

		TotalSets: atomic.LoadInt64(
			&c.metrics.TotalSets),

		Deletes: atomic.LoadInt64(
			&c.metrics.TotalDeletes),

		Entries: c.Size(),
	}
}
