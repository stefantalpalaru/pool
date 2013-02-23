/* Copyright (c) 2013, Stefan Talpalaru <stefan.talpalaru@od-eon.com>, Odeon Consulting Group Pte Ltd <od-eon.com>
 * All rights reserved. */

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

// Package pool provides a worker pool.
package pool

import (
	"fmt"
	"log"
	"time"
)

// Job holds all the data related to a worker's instance.
type Job struct {
	f      func(...interface{}) interface{}
	args   []interface{}
	Result interface{}
	Err    error
}

// stats is a structure holding statistical data about the pool
type stats struct {
	Submitted int
	Running   int
	Completed int
}

// Pool is the main data structure.
type Pool struct {
	started            bool
	num_workers        int
	job_pipe           chan *Job
	done_pipe          chan *Job
	add_pipe           chan *Job
	result_pipe        chan *Job
	jobs_ready_to_run  []*Job
	num_jobs_submitted int
	num_jobs_running   int
	num_jobs_completed int
	jobs_completed     []*Job
	interval           time.Duration // for sleeping, in ms
	working_pipe       chan bool
	stats_pipe         chan stats
}

// subworker catches any panic while running the job.
func (pool *Pool) subworker(job *Job) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("panic while running job:", err)
			job.Result = nil
			job.Err = fmt.Errorf(err.(string))
		}
	}()
	job.Result = job.f(job.args...)
}

// worker gets a job from the job_pipe, passes it to a
// subworker and puts the job in the done_pipe when finished.
func (pool *Pool) worker(num int) {
	for {
		job := <-pool.job_pipe
		pool.subworker(job)
		pool.done_pipe <- job
	}
}

// NewPool creates a new Pool.
func NewPool(workers int) (pool *Pool) {
	pool = new(Pool)
	pool.num_workers = workers
	pool.job_pipe = make(chan *Job)
	pool.done_pipe = make(chan *Job)
	pool.add_pipe = make(chan *Job)
	pool.result_pipe = make(chan *Job)
	pool.jobs_ready_to_run = make([]*Job, 0)
	pool.jobs_completed = make([]*Job, 0)
	pool.working_pipe = make(chan bool)
	pool.stats_pipe = make(chan stats)
	pool.interval = 1
	for i := 0; i < workers; i++ {
		go pool.worker(i)
	}
	return
}

// supervisor feeds jobs to workers and keeps track of them.
func (pool *Pool) supervisor() {
	pool.started = true
	for {
		select {
		case job := <-pool.add_pipe:
			pool.jobs_ready_to_run = append(pool.jobs_ready_to_run, job)
			pool.num_jobs_submitted++
		default:
		}

		num_ready_jobs := len(pool.jobs_ready_to_run)
		if num_ready_jobs > 0 {
			select {
			case pool.job_pipe <- pool.jobs_ready_to_run[num_ready_jobs-1]:
				pool.num_jobs_running++
				pool.jobs_ready_to_run = pool.jobs_ready_to_run[:num_ready_jobs-1]
			default:
			}
		}

		if pool.num_jobs_running > 0 {
			select {
			case job := <-pool.done_pipe:
				pool.num_jobs_running--
				pool.jobs_completed = append(pool.jobs_completed, job)
				pool.num_jobs_completed++
			default:
			}
		}

		working := true
		if len(pool.jobs_ready_to_run) == 0 && pool.num_jobs_running == 0 {
			working = false
		}
		select {
		case pool.working_pipe <- working:
		default:
		}

		res := (*Job)(nil)
		if len(pool.jobs_completed) > 0 {
			res = pool.jobs_completed[0]
		}
		select {
		case pool.result_pipe <- res:
			if len(pool.jobs_completed) > 0 {
				pool.jobs_completed = pool.jobs_completed[1:]
			}
		default:
		}

		pool_stats := stats{pool.num_jobs_submitted, pool.num_jobs_running, pool.num_jobs_completed}
		select {
		case pool.stats_pipe <- pool_stats:
		default:
		}

		time.Sleep(pool.interval * time.Millisecond)
	}
}

// Run starts the Pool by launching a supervisor goroutine.
// It's OK to start an empty Pool. The jobs will be fed to the workers as soon
// as they become available.
func (pool *Pool) Run() {
	go pool.supervisor()
}

// Add creates a Job from the given function and args and
// adds it to the Pool.
func (pool *Pool) Add(f func(...interface{}) interface{}, args ...interface{}) {
	pool.add_pipe <- &Job{f, args, nil, nil}
}

// Wait blocks until all the jobs in the Pool are done.
func (pool *Pool) Wait() {
	for <-pool.working_pipe {
		time.Sleep(pool.interval * time.Millisecond)
	}
}

// Results retrieves the completed jobs.
func (pool *Pool) Results() (res []*Job) {
	res = make([]*Job, len(pool.jobs_completed))
	for i, job := range pool.jobs_completed {
		res[i] = job
	}
	pool.jobs_completed = pool.jobs_completed[0:0]
	return
}

// WaitForJob blocks until a completed job is available and returns it.
// If there are no jobs running, it returns nil.
func (pool *Pool) WaitForJob() *Job {
	for {
		working := <-pool.working_pipe
		r := <-pool.result_pipe
		if r == (*Job)(nil) {
			if !working {
				break
			}
		} else {
			return r
		}
	}
	return nil
}

// Status returns a "stats" instance
func (pool *Pool) Status() stats {
	if pool.started {
		return <-pool.stats_pipe
	}
	// the pool wasn't started so we return a zeroed structure
	return stats{}
}
