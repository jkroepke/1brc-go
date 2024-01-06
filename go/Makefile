.PHONY: clean build run run-fast
build: clean default.pgo 1brc-go

clean:
	rm -f default.pgo 1brc-go

default.pgo:
	CGO_ENABLED=0 go test -count 10 -cpuprofile default.pgo -bench . > /dev/null

1brc-go:
	CGO_ENABLED=0 go build -pgo=default.pgo .

run: build
	./1brc-go ../measurements1000000000.txt

run-fast: build
	./1brc-go ../measurements100000000.txt
