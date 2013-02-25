This is a generic worker pool for the Go language. It's useful when you want to limit the number of goroutines running in parallel.

Installing (optional):
```
go install github.com/stefantalpalaru/pool
```

Compile and run the examples:
```
go run examples/pool_example.go
go run examples/web_crawler.go
```

The last example is actually an [exercise from the Go tour][1] modified to use a worker pool for fetching and processing the URLs. The need to limit the number of concurrent requests in real web scraping scenarios was what prompted the creation of this package.

[1]: http://tour.golang.org/#70

