/* Copyright (c) 2013, Stefan Talpalaru <stefan.talpalaru@od-eon.com>, Odeon Consulting Group Pte Ltd <od-eon.com>
 * All rights reserved. */

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package main

import (
	"pool"
	"runtime"
	"log"
	"math"
)

func worker(args ...interface{}) interface{} {
	/*panic("test panic")*/
	x := args[0].(float64)
	j := 0.
	for i:=1.0; i<10000000; i++ {
		j += math.Sqrt(i)
	}
	return x * x + j
}

func main() {
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)

	mypool := pool.NewPool(cpus)
	mypool.Run()
	num_jobs := float64(1000)

	for i:=float64(0); i<num_jobs; i++ {
		mypool.Add(worker, i)
	}
	status := mypool.Status()
	log.Println(status.Submitted, "submitted jobs,", status.Running, "running,", status.Completed, "completed.")
	mypool.Wait()
	sum := float64(0)
	completed_jobs := mypool.Results()
	for _, job := range(completed_jobs) {
		if job.Result == nil {
			log.Println("got error:", job.Err)
		} else {
			sum += job.Result.(float64)
		}
	}
	log.Println(sum)

	// use one result at a time as it becomes available
	for i:=float64(0); i<num_jobs; i++ {
		mypool.Add(worker, i)
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
	log.Println(sum)
}

