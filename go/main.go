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
	execute(os.Args[1])
}

func execute(fileName string) {
	data, closer := openFile(fileName)
	defer closer()

	var (
		id        uint64
		pos       int
		off       int
		stationID uint64
	)

	// get all station names, assume all station are in the first 5_000_000 lines
	for pos <= 5_000_000 {
		for j, c := range data[pos:] {
			if c == ';' {
				off = j
				break
			}
		}

		stationID = maphash.Bytes(maphashSeed, data[pos:pos+off])
		if _, ok := stationSymbolMap[stationID]; !ok {
			stationNames = append(stationNames, string(data[pos:pos+off]))
			stationSymbolMap[stationID] = id
			id++
		}

		pos += off + 2

		if data[pos+2] == '.' {
			// -21.3\n
			pos += 5
		} else if data[pos+1] == '.' {
			// 21.3\n or -1.3\n
			pos += 4
		} else if data[pos] == '.' {
			// 1.3\n
			pos += 3
		}
	}

	workerSize := len(data) / workerCount

	wg := sync.WaitGroup{}
	for workerID := 0; workerID < workerCount; workerID++ {
		wg.Add(1)

		// process data in parallel
		go func(workerID int) {
			defer wg.Done()

			var (
				pos         int
				off         int
				stationID   uint64
				temperature int64
			)

			last := workerSize*(workerID+1) + 20
			if last > len(data) {
				last = len(data) - 1
			}

			data := data[workerSize*workerID : last]
			data = data[bytes.IndexByte(data, '\n')+1 : bytes.LastIndexByte(data, '\n')+1]

			for {
				// find semicolon to get station name
				off = -1

				for j, c := range data[pos:] {
					if c == ';' {
						off = j
						break
					}
				}

				if off == -1 {
					break
				}

				// translate station name to station ID
				stationID = stationSymbolMap[maphash.Bytes(maphashSeed, data[pos:pos+off])]
				pos += off + 1

				// parse temperature
				{
					negative := data[pos] == '-'
					if negative {
						pos++
					}

					if data[pos+1] == '.' {
						// 1.2\n
						temperature = int64(data[pos+2]) + int64(data[pos+0])*10 - '0'*(11)
						pos += 4
					} else {
						// 12.3\n
						temperature = int64(data[pos+3]) + int64(data[pos+1])*10 + int64(data[pos+0])*100 - '0'*(111)
						pos += 5
					}

					if negative {
						temperature = -temperature
					}
				}

				results[workerID][stationID].count++
				results[workerID][stationID].sum += temperature
				if temperature < results[workerID][stationID].min {
					results[workerID][stationID].min = temperature
				}
				if temperature > results[workerID][stationID].max {
					results[workerID][stationID].max = temperature
				}
			}
		}(workerID)
	}

	// wait for all workers to finish
	wg.Wait()

	// merge results
	for _, result := range results {
		for stationID, stationResult := range result {
			if stationResult.count == 0 {
				continue
			}

			stationResultMap[stationID].sum += stationResult.sum
			stationResultMap[stationID].count += stationResult.count
			if stationResult.min < stationResultMap[stationID].min {
				stationResultMap[stationID].min = stationResult.min
			}
			if stationResult.max > stationResultMap[stationID].max {
				stationResultMap[stationID].max = stationResult.max
			}
		}
	}

	// sort station names
	slices.Sort(stationNames)

	fmt.Print("{")

	var result stationResult

	// Print results {station1=min/avg/max, station2=min/avg/max, ...}
	for i, station := range stationNames {
		if i != 0 {
			fmt.Print(", ")
		}

		result = stationResultMap[stationSymbolMap[maphash.String(maphashSeed, station)]]
		fmt.Printf("%s=%.1f/%.1f/%.1f",
			station,
			float64(result.min)/10,
			math.Round(float64(result.sum)/float64(result.count))/10.0,
			float64(result.max)/10,
		)
	}

	fmt.Print("}\n")
}

// openFile uses syscall.Mmap to read file into memory.
func openFile(fileName string) ([]byte, func()) {
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}

	stat, _ := f.Stat()
	size := stat.Size()

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatalf("Mmap: %v", err)
	}

	return data, func() { _ = syscall.Munmap(data); _ = f.Close() }
}
