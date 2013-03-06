/* Copyright (c) 2013, Stefan Talpalaru <stefan.talpalaru@od-eon.com>, Odeon Consulting Group Pte Ltd <od-eon.com>
 * All rights reserved. */

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import (
	"github.com/stefantalpalaru/pool"
	"log"
	"math"
	"runtime"
)

func work(args ...interface{}) interface{} {
	/*panic("test panic")*/
	x := args[0].(float64)
	j := 0.
	for i := 1.0; i < 10000000; i++ {
		j += math.Sqrt(i)
	}
	return x*x + j
}

func main() {
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)
	num_jobs := float64(1000)

	// classical usage: add all the jobs then wait untill all are done
	log.Println("*** classical usage ***")
	mypool := pool.New(cpus)
	mypool.Run()
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	status := mypool.Status()
	log.Println("after adding all the jobs:")
	log.Println(status.Submitted, "submitted jobs,", status.Running, "running,", status.Completed, "completed.")
	mypool.Wait()
	sum := float64(0)
	completed_jobs := mypool.Results()
	for _, job := range completed_jobs {
		if job.Result == nil {
			log.Println("got error:", job.Err)
		} else {
			sum += job.Result.(float64)
		}
	}
	log.Println(sum)
	mypool.Stop()

	// alternative scenario: use one result at a time as it becomes available
	log.Println("*** using one result at a time as it becomes available ***")
	mypool = pool.New(cpus)
	mypool.Run()
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	sum = float64(0)
	for {
		job := mypool.WaitForJob()
		if job == nil {
			break
		}
		if job.Result == nil {
			log.Println("got error:", job.Err)
		} else {
			sum += job.Result.(float64)
		}
	}
	status = mypool.Status()
	log.Println("after getting all the results:")
	log.Println(status.Submitted, "submitted jobs,", status.Running, "running,", status.Completed, "completed.")
	log.Println(sum)
	mypool.Stop()

	// stopping and restarting the pool
	log.Println("*** stopping and restarting the pool ***")
	mypool = pool.New(cpus)
	mypool.Run()
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	sum = float64(0)
	status = mypool.Status()
	mypool.Stop()
	log.Println("after stopping:")
	log.Println(status.Submitted, "submitted jobs,", status.Running, "running,", status.Completed, "completed.")
	mypool.Run()
	mypool.Wait()
	completed_jobs = mypool.Results()
	for _, job := range completed_jobs {
		if job.Result == nil {
			log.Println("got error:", job.Err)
		} else {
			sum += job.Result.(float64)
		}
	}
	status = mypool.Status()
	log.Println("after restarting and getting all the results:")
	log.Println(status.Submitted, "submitted jobs,", status.Running, "running,", status.Completed, "completed.")
	log.Println(sum)
	mypool.Stop()
}
