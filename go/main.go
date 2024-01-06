package main

import (
	"bytes"
	"fmt"
	"golang.org/x/exp/slices"
	"hash/maphash"
	"log"
	"math"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	workerCount         = 12 // set this value to the number of CPU cores
	numberOfMaxStations = 10_000
)

var (
	maphashSeed = maphash.MakeSeed()

	results          = [workerCount][numberOfMaxStations]stationResult{}
	stationNames     = make([]string, 0, numberOfMaxStations)
	stationSymbolMap = make(map[uint64]uint64, numberOfMaxStations)
	stationResultMap = [numberOfMaxStations]stationResult{}
)

type stationResult struct {
	count int64
	min   int64
	max   int64
	sum   int64
}

func main() {
	start := time.Now()

	execute(os.Args[1])

	_, _ = fmt.Fprintf(os.Stderr, "%dms\n", time.Since(start).Milliseconds())
}

func execute(fileName string) {
	data, closer := openFile(fileName)
	defer closer()

	var (
		id    uint64
		index int
		pos   int
	)

	// get all station names, assume all station are in the first 5_000_000 lines
	for {
		index = bytes.IndexByte(data, ';')
		pos += index + 1
		if pos >= 5_000_000 {
			break
		}

		stationID := maphash.Bytes(maphashSeed, data[:index])
		if _, ok := stationSymbolMap[stationID]; !ok {
			stationNames = append(stationNames, string(data[:index]))
			stationSymbolMap[stationID] = id
			id++
		}

		index = bytes.IndexByte(data, '\n')
		pos += index + 1

		data = data[index+1:]
	}

	workerSize := len(data) / workerCount

	wg := sync.WaitGroup{}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				index       int
				stationID   uint64
				temperature int64
			)

			last := workerSize*(i+1) + 20
			if last > len(data) {
				last = len(data) - 1
			}

			data := data[workerSize*i : last]
			data = data[:bytes.LastIndexByte(data, '\n')+1]

			for {
				// find semicolon to get station name
				index = bytes.IndexByte(data, ';')
				if index == -1 {
					break
				}

				// translate station name to station ID
				stationID = stationSymbolMap[maphash.Bytes(maphashSeed, data[:index])]
				data = data[index+1:]

				// find newline to get temperature
				index = bytes.IndexByte(data, '\n')

				// parse temperature
				temperature = parseNumber(data[:index])
				data = data[index+1:]

				results[i][stationID].count++
				results[i][stationID].sum += temperature
				if temperature < results[i][stationID].min {
					results[i][stationID].min = temperature
				}
				if temperature > results[i][stationID].max {
					results[i][stationID].max = temperature
				}
			}
		}(i)
	}

	wg.Wait()

	for _, result := range results {
		for station, stationResult := range result {
			if stationResult.count == 0 {
				continue
			}

			stationResultMap[station].sum += stationResult.sum
			stationResultMap[station].count += stationResult.count
			if stationResult.min < stationResultMap[station].min {
				stationResultMap[station].min = stationResult.min
			}
			if stationResult.max > stationResultMap[station].max {
				stationResultMap[station].max = stationResult.max
			}
		}
	}

	/*
		sort.Slice(stationNames, func(i, j int) bool {
			return bytes.Compare(stationNames[i], stationNames[j]) < 0
		})
	*/
	slices.Sort(stationNames)

	fmt.Print("{")

	for i, station := range stationNames {
		if i != 0 {
			fmt.Print(", ")
		}

		stationID := stationSymbolMap[maphash.String(maphashSeed, station)]
		result := stationResultMap[stationID]
		fmt.Printf("%s=%.1f/%.1f/%.1f",
			station,
			round(float64(result.min)/10.0),
			round(float64(result.sum)/10.0/float64(result.count)),
			round(float64(result.max)/10.0),
		)
	}

	fmt.Print("}\n")
}

func round(x float64) float64 {
	return math.Round(x*10.0) / 10.0
}

// openFile uses syscall.Mmap to read file into memory.
func openFile(fileName string) ([]byte, func()) {
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	stat, _ := f.Stat()
	size := stat.Size()

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatalf("Mmap: %v", err)
	}

	return data, func() { syscall.Munmap(data) }
}

// parseNumber reads decimal number that matches "^-?[0-9]{1,2}[.][0-9]" pattern,
// e.g.: -12.3, -3.4, 5.6, 78.9 and return the value*10, i.e. -123, -34, 56, 789.
func parseNumber(data []byte) int64 {
	negative := data[0] == '-'
	if negative {
		data = data[1:]
	}

	var result int64
	switch len(data) {
	// 1.2
	case 3:
		result = int64(data[0])*10 + int64(data[2]) - '0'*(10+1)
	// 12.3
	case 4:
		result = int64(data[0])*100 + int64(data[1])*10 + int64(data[3]) - '0'*(100+10+1)
	}

	if negative {
		return -result
	}
	return result
}
