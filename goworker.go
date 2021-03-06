package goworker

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cihub/seelog"
	"github.com/garyburd/redigo/redis"
)

const redisConnectionBuffer = 5

var (
	logger seelog.LoggerInterface
	pool   *redis.Pool
)

// Init initializes the goworker process. This will be
// called by the Work function, but may be used by programs
// that wish to access goworker functions and configuration
// without actually processing jobs.
func Init() error {
	var err error
	logger, err = seelog.LoggerFromWriterWithMinLevel(os.Stdout, seelog.InfoLvl)
	if err != nil {
		return err
	}

	if err := flags(); err != nil {
		return err
	}

	pool = newRedisPool(uri, connections, connections+redisConnectionBuffer, time.Minute)

	return nil
}

// GetConn returns a connection from the goworker Redis
// connection pool. When using the pool, check in
// connections as quickly as possible, because holding a
// connection will cause concurrent worker functions to lock
// while they wait for an available connection. Expect this
// API to change drastically.
func GetConn() (redis.Conn, error) {
	r := pool.Get()

	// Force acquisition of an underlying connection:
	// https://github.com/garyburd/redigo/blob/master/redis/pool.go#L138
	if err := r.Err(); err != nil {
		r.Close()
		return nil, err
	}

	return r, nil
}

// PutConn puts a connection back into the connection pool.
// Run this as soon as you finish using a connection that
// you got from GetConn. Expect this API to change
// drastically.
func PutConn(conn redis.Conn) {
	if conn != nil {
		conn.Close()
	}
}

// Close cleans up resources initialized by goworker. This
// will be called by Work when cleaning up. However, if you
// are using the Init function to access goworker functions
// and configuration without processing jobs by calling
// Work, you should run this function when cleaning up. For
// example,
//
//	if err := goworker.Init(); err != nil {
//		fmt.Println("Error:", err)
//	}
//	defer goworker.Close()
func Close() {
	if pool != nil {
		pool.Close()
	}
}

// Work starts the goworker process. Check for errors in
// the return value. Work will take over the Go executable
// and will run until a QUIT, INT, or TERM signal is
// received, or until the queues are empty if the
// -exit-on-complete flag is set.
func Work() error {
	err := Init()
	if err != nil {
		return err
	}
	defer Close()

	quit := Signals()

	poller, err := newPoller(queues, isStrict)
	if err != nil {
		return err
	}

	pollInterval := time.Duration(interval)
	var jobs <-chan *job
	for {
		var err error
		jobs, err = poller.beginPolling(pollInterval, quit)
		if err == nil {
			logger.Infof("Poller initialized %s", poller)
			break
		}

		// Sleep for a bit or wait for the quit signal
		ticker := time.After(pollInterval)
		select {
		case <-quit:
			return nil
		case <-ticker:
		}
	}

	var monitor sync.WaitGroup

	for id := 0; id < concurrency; id++ {
		worker, err := newWorker(strconv.Itoa(id), queues)
		if err != nil {
			return err
		}
		worker.work(jobs, &monitor)
	}

	monitor.Wait()

	return nil
}
