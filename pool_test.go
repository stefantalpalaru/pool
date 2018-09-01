/* Copyright (c) 2013-2018 - Ștefan Talpalaru <stefantalpalaru@yahoo.com>
 * All rights reserved. */

/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package pool

import (
	"math"
	"runtime"
	"testing"
)

func work(args ...interface{}) interface{} {
	x := args[0].(float64)
	j := 0.
	for i := 1.0; i < 10000000; i++ {
		j += math.Sqrt(i)
	}
	return x*x + j
}

func processResults(t *testing.T, results []*Job) (sum float64) {
	for _, job := range results {
		if job.Result == nil {
			t.Error("got error:", job.Err)
		} else {
			sum += job.Result.(float64)
		}
	}
	return
}

func processResultsWhenAvailable(t *testing.T, mypool *Pool) (sum float64) {
	for {
		job := mypool.WaitForJob()
		if job == nil {
			break
		}
		if job.Result == nil {
			t.Error("got error:", job.Err)
		} else {
			sum += job.Result.(float64)
		}
	}
	return
}

func validateResult(t *testing.T, result, reference float64, error_msg string) {
	if result != reference {
		t.Error(result, "!=", reference, error_msg)
	}
}

func TestCorrectness(t *testing.T) {
	num_jobs := float64(50)
	runtime.GOMAXPROCS(5) // number of OS threads

	// without the pool
	reference := float64(0)
	for i := float64(0); i < num_jobs; i++ {
		reference += work(i).(float64)
	}

	// 1 worker, add before running
	mypool := New(1)
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	mypool.Run()
	mypool.Wait()
	validateResult(t, processResults(t, mypool.Results()), reference, "1 worker, add before running")
	mypool.Stop()

	// 1 worker, run before adding
	mypool = New(1)
	mypool.Run()
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	mypool.Wait()
	validateResult(t, processResults(t, mypool.Results()), reference, "1 worker, run before adding")
	mypool.Stop()

	// 10 workers, add before running
	mypool = New(10)
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	mypool.Run()
	mypool.Wait()
	validateResult(t, processResults(t, mypool.Results()), reference, "10 workers, add before running")
	mypool.Stop()

	// 10 workers, run before adding
	mypool = New(10)
	mypool.Run()
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	mypool.Wait()
	validateResult(t, processResults(t, mypool.Results()), reference, "10 workers, run before adding")
	mypool.Stop()

	// process results as soon as they are available (add before running)
	mypool = New(10)
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	mypool.Run()
	validateResult(t, processResultsWhenAvailable(t, mypool), reference, "process results as soon as they are available (add before running)")
	mypool.Stop()

	// process results as soon as they are available (run before adding)
	mypool = New(10)
	mypool.Run()
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	validateResult(t, processResultsWhenAvailable(t, mypool), reference, "process results as soon as they are available (add before running)")
	mypool.Stop()

	// stop/start the pool
	mypool = New(10)
	mypool.Run()
	for i := float64(0); i < num_jobs; i++ {
		mypool.Add(work, i)
	}
	mypool.Stop()
	mypool.Run()
	validateResult(t, processResultsWhenAvailable(t, mypool), reference, "stop/start the pool")
	mypool.Stop()
}
