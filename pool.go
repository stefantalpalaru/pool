/* Copyright (c) 2013-2014 - Stefan Talpalaru <stefantalpalaru@yahoo.com>
 * All rights reserved. */

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

// Package pool provides a worker pool.
package pool

import (
	"container/list"
	"fmt"
	"log"
	"sync"
	"time"
)

// Job holds all the data related to a worker's instance.
type Job struct {
	F      func(...interface{}) interface{}
	Args   []interface{}
	Result interface{}
	Err    error
	added  chan bool // used by Pool.Add to wait for the supervisor
}

// stats is a structure holding statistical data about the pool.
type stats struct {
	Submitted int
	Running   int
	Completed int
}

// Pool is the main data structure.
type Pool struct {
	workers_started      bool
	supervisor_started   bool
	num_workers          int
	job_wanted_pipe      chan chan *Job
	done_pipe            chan *Job
	add_pipe             chan *Job
	result_wanted_pipe   chan chan *Job
	jobs_ready_to_run    *list.List
	num_jobs_submitted   int
	num_jobs_running     int
	num_jobs_completed   int
	jobs_completed       *list.List
	interval             time.Duration // for sleeping, in ms
	working_wanted_pipe  chan chan bool
	stats_wanted_pipe    chan chan stats
	worker_kill_pipe     chan bool
	supervisor_kill_pipe chan bool
	worker_wg            sync.WaitGroup
	supervisor_wg        sync.WaitGroup
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
	job.Result = job.F(job.Args...)
}

// worker gets a job from the job_pipe, passes it to a
// subworker and puts the job in the done_pipe when finished.
func (pool *Pool) worker(num int) {
	job_pipe := make(chan *Job)
WORKER_LOOP:
	for {
		pool.job_wanted_pipe <- job_pipe
		job := <-job_pipe
		if job == nil {
			time.Sleep(pool.interval * time.Millisecond)
		} else {
			pool.subworker(job)
			pool.done_pipe <- job
		}
		select {
		case <-pool.worker_kill_pipe:
			break WORKER_LOOP
		default:
		}
	}
	pool.worker_wg.Done()
}

// New creates a new Pool.
func New(workers int) (pool *Pool) {
	pool = new(Pool)
	pool.num_workers = workers
	pool.job_wanted_pipe = make(chan chan *Job)
	pool.done_pipe = make(chan *Job)
	pool.add_pipe = make(chan *Job)
	pool.result_wanted_pipe = make(chan chan *Job)
	pool.jobs_ready_to_run = list.New()
	pool.jobs_completed = list.New()
	pool.working_wanted_pipe = make(chan chan bool)
	pool.stats_wanted_pipe = make(chan chan stats)
	pool.worker_kill_pipe = make(chan bool)
	pool.supervisor_kill_pipe = make(chan bool)
	pool.interval = 1
	// start the supervisor here so we can accept jobs before a Run call
	pool.startSupervisor()
	return
}

// supervisor feeds jobs to workers and keeps track of them.
func (pool *Pool) supervisor() {
SUPERVISOR_LOOP:
	for {
		select {
		// new job
		case job := <-pool.add_pipe:
			pool.jobs_ready_to_run.PushBack(job)
			pool.num_jobs_submitted++
			job.added <- true
		// send jobs to the workers
		case job_pipe := <-pool.job_wanted_pipe:
			element := pool.jobs_ready_to_run.Front()
			var job *Job = nil
			if element != nil {
				job = element.Value.(*Job)
				pool.num_jobs_running++
				pool.jobs_ready_to_run.Remove(element)
			}
			job_pipe <- job
		// job completed
		case job := <-pool.done_pipe:
			pool.num_jobs_running--
			pool.jobs_completed.PushBack(job)
			pool.num_jobs_completed++
		// wait for job
		case result_pipe := <-pool.result_wanted_pipe:
			close_pipe := false
			job := (*Job)(nil)
			element := pool.jobs_completed.Front()
			if element != nil {
				job = element.Value.(*Job)
				pool.jobs_completed.Remove(element)
			} else {
				if pool.num_jobs_running == 0 && pool.num_jobs_completed == pool.num_jobs_submitted {
					close_pipe = true
				}
			}
			if close_pipe {
				close(result_pipe)
			} else {
				result_pipe <- job
			}
		// is the pool working or just lazing on a Sunday afternoon?
		case working_pipe := <-pool.working_wanted_pipe:
			working := true
			if pool.jobs_ready_to_run.Len() == 0 && pool.num_jobs_running == 0 {
				working = false
			}
			working_pipe <- working
		// stats
		case stats_pipe := <-pool.stats_wanted_pipe:
			pool_stats := stats{pool.num_jobs_submitted, pool.num_jobs_running, pool.num_jobs_completed}
			stats_pipe <- pool_stats
		// stopping
		case <-pool.supervisor_kill_pipe:
			break SUPERVISOR_LOOP
		}
	}
	pool.supervisor_wg.Done()
}

// Run starts the Pool by launching the workers.
// It's OK to start an empty Pool. The jobs will be fed to the workers as soon
// as they become available.
func (pool *Pool) Run() {
	if pool.workers_started {
		panic("trying to start a pool that's already running")
	}
	for i := 0; i < pool.num_workers; i++ {
		pool.worker_wg.Add(1)
		go pool.worker(i)
	}
	pool.workers_started = true
	// handle the supervisor
	if !pool.supervisor_started {
		pool.startSupervisor()
	}
}

// Stop will signal the workers to exit and wait for them to actually do that.
// It also releases any other resources (e.g.: it stops the supervisor goroutine)
// so call this method when you're done with the Pool instance to allow the GC
// to do its job.
func (pool *Pool) Stop() {
	if !pool.workers_started {
		panic("trying to stop a pool that's already stopped")
	}
	// stop the workers
	for i := 0; i < pool.num_workers; i++ {
		pool.worker_kill_pipe <- true
	}
	pool.worker_wg.Wait()
	// set the flag
	pool.workers_started = false
	// handle the supervisor
	if pool.supervisor_started {
		pool.stopSupervisor()
	}
}

func (pool *Pool) startSupervisor() {
	pool.supervisor_wg.Add(1)
	go pool.supervisor()
	pool.supervisor_started = true
}

func (pool *Pool) stopSupervisor() {
	pool.supervisor_kill_pipe <- true
	pool.supervisor_wg.Wait()
	pool.supervisor_started = false
}

// Add creates a Job from the given function and args and
// adds it to the Pool.
func (pool *Pool) Add(f func(...interface{}) interface{}, args ...interface{}) {
	job := &Job{f, args, nil, nil, make(chan bool)}
	pool.add_pipe <- job
	<-job.added
}

// Wait blocks until all the jobs in the Pool are done.
func (pool *Pool) Wait() {
	working_pipe := make(chan bool)
	for {
		pool.working_wanted_pipe <- working_pipe
		if !<-working_pipe {
			break
		}
		time.Sleep(pool.interval * time.Millisecond)
	}
}

// Results retrieves the completed jobs.
func (pool *Pool) Results() (res []*Job) {
	res = make([]*Job, pool.jobs_completed.Len())
	i := 0
	for e := pool.jobs_completed.Front(); e != nil; e = e.Next() {
		res[i] = e.Value.(*Job)
		i++
	}
	pool.jobs_completed = list.New()
	return
}

// WaitForJob blocks until a completed job is available and returns it.
// If there are no jobs running, it returns nil.
func (pool *Pool) WaitForJob() *Job {
	result_pipe := make(chan *Job)
	var job *Job
	var ok bool
	for {
		pool.result_wanted_pipe <- result_pipe
		job, ok = <-result_pipe
		if !ok {
			// no more results available
			return nil
		}
		if job == (*Job)(nil) {
			// no result available right now but there are jobs running
			time.Sleep(pool.interval * time.Millisecond)
		} else {
			break
		}
	}
	return job
}

// Status returns a "stats" instance.
func (pool *Pool) Status() stats {
	stats_pipe := make(chan stats)
	if pool.supervisor_started {
		pool.stats_wanted_pipe <- stats_pipe
		return <-stats_pipe
	}
	// the supervisor wasn't started so we return a zeroed structure
	return stats{}
}
