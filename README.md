## description

This is a generic worker pool for the Go language. It's useful when you want to limit the number of goroutines running in parallel.

## installation

```sh
go install github.com/stefantalpalaru/pool
```

## compile and run the examples

```sh
go run examples/pool_example.go
go run examples/web_crawler.go
```

The last example is actually an [exercise from the Go tour][1] modified to use a worker pool for fetching and processing the URLs. The need to limit the number of concurrent requests in real web scraping scenarios was what prompted the creation of this package.

## documentation

You can see the godoc-generated documentation online at [godoc.org/github.com/stefantalpalaru/pool][2] but it might be a bit dry.

The best documentation is in the form of an example: [pool\_example.go](examples/pool_example.go).

## license

MPL-2.0

## credits

- author: Ștefan Talpalaru <stefantalpalaru@yahoo.com>

- homepage: https://github.com/stefantalpalaru/pool

[1]: https://tour.golang.org/concurrency/10
[2]: http://godoc.org/github.com/stefantalpalaru/pool

