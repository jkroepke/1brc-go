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
		id    uint64
		index int
		pos   int
	)

	// get all station names, assume all station are in the first 5_000_000 lines
	for {
		for j, c := range data[index:] {
			if c == ';' {
				pos = j
				break
			}
		}

		if index >= 5_000_000 {
			break
		}

		stationID := maphash.Bytes(maphashSeed, data[index:index+pos])
		if _, ok := stationSymbolMap[stationID]; !ok {
			stationNames = append(stationNames, string(data[index:index+pos]))
			stationSymbolMap[stationID] = id
			id++
		}

		index += pos + 2

		if data[index+2] == '.' {
			index += 5
		} else if data[index+1] == '.' {
			index += 4
		} else if data[index] == '.' {
			index += 3
		} else {
			panic("invalid temperature")
		}
	}

	workerSize := len(data) / workerCount

	wg := sync.WaitGroup{}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				pos         int
				stationID   uint64
				temperature int64
			)

			last := workerSize*(i+1) + 20
			if last > len(data) {
				last = len(data) - 1
			}

			data := data[workerSize*i : last]
			data = data[bytes.IndexByte(data, '\n')+1 : bytes.LastIndexByte(data, '\n')+1]

			for {
				// find semicolon to get station name
				pos = -1

				for j, c := range data {
					if c == ';' {
						pos = j
						break
					}
				}

				if pos == -1 {
					break
				}

				// translate station name to station ID
				stationID = stationSymbolMap[maphash.Bytes(maphashSeed, data[:pos])]
				data = data[pos+1:]

				// parse temperature
				{
					negative := data[0] == '-'
					if negative {
						data = data[1:]
					}

					if data[1] == '.' {
						// 1.2\n
						temperature = int64(data[2]) + int64(data[0])*10 - '0'*(11)
						data = data[4:]
						// 12.3\n
					} else {
						temperature = int64(data[3]) + int64(data[1])*10 + int64(data[0])*100 - '0'*(111)
						data = data[5:]
					}

					if negative {
						temperature = -temperature
					}
				}

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

	slices.Sort(stationNames)

	fmt.Print("{")

	var result stationResult

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
