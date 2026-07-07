package main

import (
	"fmt"
	"time"

	"github.com/bhavar12/go-cache/internal/cache"
)

func main() {

	cfg := cache.Config{
		Capacity:        100,
		CleanupInterval: 5 * time.Second,
	}

	c := cache.New[string](cfg)

	defer c.Close()

	c.SetWithttl(
		"user1",
		"John",
		3*time.Second,
	)

	time.Sleep(10 * time.Second)

	fmt.Println(c.Size())
}
