package goworker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
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

func (p *poller) getJob(conn redis.Conn) (*job, error) {
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

func (p *poller) beginPolling(interval time.Duration, quit <-chan bool) (<-chan *job, error) {
	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection to initialize poller %s", p)
		return nil, err
	}

	p.open(conn)
	p.start(conn)
	PutConn(conn)

	jobs := make(chan *job)

	go func() {
		defer func() {
			close(jobs)

			conn, err := GetConn()
			if err != nil {
				logger.Criticalf("Error on getting connection for cleanup in poller %s", p)
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
					ticker := time.After(interval)
					select {
					case <-quit:
						return
					case <-ticker:
						continue
					}
				}

				job, err := p.getJob(conn)
				if err != nil {
					logger.Errorf("Error on %v getting job from %v: %v", p, p.Queues, err)
					PutConn(conn)
					continue
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
							logger.Criticalf("Error on getting connection for requeue in poller %s", p)
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

	return jobs, nil
}
