.PHONY: clean build run run-fast
build: clean default.pgo 1brc-go

clean:
	rm -f default.pgo 1brc-go

default.pgo:
	CGO_ENABLED=0 go test -bench '^\QBenchmarkReal\E$$' -run '^$$' -count 1 -cpuprofile default.pgo -bench . > /dev/null

result.txt:
	../calculate_average_baseline.sh > result.txt

1brc-go:
	CGO_ENABLED=0 go build -gcflags=-B -pgo=default.pgo .

run: build
	GOGC=off ./1brc-go ../measurements.txt > /dev/null
	sleep 6 // CPU throttling
	time GOGC=off ./1brc-go ../measurements.txt > /dev/null
	sleep 6 // CPU throttling
	time GOGC=off ./1brc-go ../measurements.txt > /dev/null
	sleep 6 // CPU throttling
	time GOGC=off ./1brc-go ../measurements.txt > /dev/null
	sleep 6 // CPU throttling
	time GOGC=off ./1brc-go ../measurements.txt > /dev/null
	sleep 10 // CPU throttling
	time GOGC=off ./1brc-go ../measurements.txt > result.txt

run-fast: build
	./1brc-go ../measurements.txt

test: result.txt run
	diff expected.txt result.txt
