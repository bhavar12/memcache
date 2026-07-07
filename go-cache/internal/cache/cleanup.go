package cache

import "time"

func (c *Cache[T]) startCleanup(interval time.Duration) {

	c.wg.Add(1)

	go func() {

		defer c.wg.Done()

		ticker := time.NewTicker(interval)

		defer ticker.Stop()

		for {

			select {

			case <-ticker.C:

				c.removeExpired()

			case <-c.ctx.Done():

				return
			}
		}
	}()
}
