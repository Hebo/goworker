package goworker

import (
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/soveran/redisurl"
)

func newRedisPool(uri string, maxIdle int, maxOpen int, idleTimout time.Duration) *redis.Pool {
	generator := func() (redis.Conn, error) {
		return redisurl.ConnectToURL(uri)
	}

	p := redis.NewPool(generator, maxIdle)
	p.MaxActive = maxOpen
	p.IdleTimeout = idleTimout

	return p
}
