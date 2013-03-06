// Exercise: Web Crawler - http://tour.golang.org/#70
// modified to use the worker pool
package main

import (
	"fmt"
	"github.com/stefantalpalaru/pool"
	"runtime"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

type crawlResult struct {
	body string
	urls []string
	err error
}

// work uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func work(args ...interface{}) interface{} {
	url := args[0].(string)
	depth := args[1].(int)
	fetcher := args[2].(Fetcher)
	if depth <= 0 {
		return crawlResult{}
	}
	body, urls, err := fetcher.Fetch(url)
	return crawlResult{body, urls, err}
}

func url_already_processed(urls []string, u string) bool {
	for _, url := range urls {
		if url == u {
			return true
		}
	}
	return false
}

var mypool = pool.New(6) // number of workers

func main() {
	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)
	mypool.Run()
	first_url := "http://golang.org/"
	seen_urls := []string{first_url}
	mypool.Add(work, first_url, 4, fetcher)
	for {
		job := mypool.WaitForJob()
		if job == nil {
			break
		}
		if job.Result == nil {
			fmt.Println("got error:", job.Err)
		} else {
			result := job.Result.(crawlResult)
			if result.err != nil {
				fmt.Println(result.err)
			} else {
				url := job.Args[0].(string)
				depth := job.Args[1].(int)
				if depth <= 0 {
					continue
				}
				fmt.Printf("found: %s %q\n", url, result.body)
				for _, u := range result.urls {
					if url_already_processed(seen_urls, u) {
						continue
					}
					mypool.Add(work, u, depth-1, fetcher)
					seen_urls = append(seen_urls, u)
				}
			}
		}
	}
	mypool.Stop()
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f *fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := (*f)[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = &fakeFetcher{
	"http://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"http://golang.org/pkg/",
			"http://golang.org/cmd/",
		},
	},
	"http://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"http://golang.org/",
			"http://golang.org/cmd/",
			"http://golang.org/pkg/fmt/",
			"http://golang.org/pkg/os/",
		},
	},
	"http://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
	"http://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
}
