package goworker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

type poller struct {
	process
	isStrict bool
}

func newPoller(queues []string, isStrict bool) (*poller, error) {
	process, err := newProcess("poller", queues)
	if err != nil {
		return nil, err
	}
	return &poller{
		process:  *process,
		isStrict: isStrict,
	}, nil
}

func (p *poller) getJob(conn *RedisConn) (*job, error) {
	for _, queue := range p.queues(p.isStrict) {
		logger.Debugf("Checking %s", queue)

		reply, err := conn.Do("LPOP", fmt.Sprintf("%squeue:%s", namespace, queue))
		if err != nil {
			return nil, err
		}
		if reply != nil {
			logger.Debugf("Found job on %s", queue)

			job := &job{Queue: queue}

			decoder := json.NewDecoder(bytes.NewReader(reply.([]byte)))
			if useNumber {
				decoder.UseNumber()
			}

			if err := decoder.Decode(&job.Payload); err != nil {
				return nil, err
			}
			return job, nil
		}
	}

	return nil, nil
}

// initialize() runs in an endless loop until an initial connection is
// established or a quit signal is received. This will effectively block
// the running routine until a connection is available or a quit signal is
// received.
func (p *poller) initialize(interval time.Duration, quit <-chan bool) bool {
	for {
		conn, err := GetConn()
		if err != nil {
			logger.Criticalf("Error on getting connection to initialize poller %s", p)

			// Sleep for a bit or wait for the quit signal
			ticker := time.After(interval)
			select {
			case <-quit:
				return false
			case <-ticker:
				continue // try again
			}
		}

		p.open(conn)
		p.start(conn)
		PutConn(conn)

		return true
	}
}

func (p *poller) poll(interval time.Duration, quit <-chan bool) <-chan *job {
	if ok := p.initialize(interval, quit); !ok {
		// The only way for this to happen is a quit signal being received,
		// which means we're shutting down anyway...
		return nil
	}

	jobs := make(chan *job)

	go func() {
		defer func() {
			close(jobs)

			conn, err := GetConn()
			if err != nil {
				logger.Criticalf("Error on getting connection in poller %s", p)
				return
			}

			p.finish(conn)
			p.close(conn)
			PutConn(conn)
		}()

		for {
			select {
			case <-quit:
				return
			default:
				conn, err := GetConn()
				if err != nil {
					logger.Criticalf("Error on getting connection in poller %s", p)
					return
				}

				job, err := p.getJob(conn)
				if err != nil {
					logger.Errorf("Error on %v getting job from %v: %v", p, p.Queues, err)
					return
				}

				if job != nil {
					conn.Send("INCR", fmt.Sprintf("%sstat:processed:%v", namespace, p))
					conn.Flush()
					PutConn(conn)
					select {
					case jobs <- job:
					case <-quit:
						buf, err := json.Marshal(job.Payload)
						if err != nil {
							logger.Criticalf("Error requeueing %v: %v", job, err)
							return
						}

						conn, err := GetConn()
						if err != nil {
							logger.Criticalf("Error on getting connection in poller %s", p)
							return
						}

						conn.Send("LPUSH", fmt.Sprintf("%squeue:%s", namespace, job.Queue), buf)
						conn.Flush()
						return
					}
				} else {
					PutConn(conn)
					if exitOnComplete {
						return
					}
					logger.Debugf("Sleeping for %v", interval)
					logger.Debugf("Waiting for %v", p.Queues)

					timeout := time.After(interval)
					select {
					case <-quit:
						return
					case <-timeout:
					}
				}
			}
		}
	}()

	return jobs
}
