module github.com/placeholder/xlru/benchmarks

go 1.27.0

require (
	github.com/phuslu/lru v1.0.22
	github.com/placeholder/xlru v0.0.0
)

require golang.org/x/sync v0.19.0 // indirect

replace github.com/placeholder/xlru => ..

replace github.com/phuslu/lru => github.com/yeqiaojun/lru v1.0.22
