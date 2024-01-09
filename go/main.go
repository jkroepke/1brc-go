package main

import (
	"bytes"
	"hash/maphash"
	"os"
	"slices"
	"strconv"
	"syscall"
)

const (
	workerCount         = 12 // set this value to the number of CPU cores
	numberOfMaxStations = 10_000
)

var maphashSeed = maphash.MakeSeed()

type WorkerResults [workerCount][numberOfMaxStations]stationResult

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
	var (
		workerResults    = WorkerResults{}
		stationNames     = make([][]byte, 0, numberOfMaxStations)
		stationResults   = [numberOfMaxStations]stationResult{}
		stationSymbolMap = make(map[uint64]uint64, numberOfMaxStations)
	)

	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	stat, _ := f.Stat()
	size := stat.Size()

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(data)

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
			stationNames = append(stationNames, data[pos:pos+off])
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

	done := make(chan struct{}, workerCount)

	go func() {
		// sort station names
		slices.SortFunc(stationNames, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})

		done <- struct{}{}
	}()

	for workerID := 0; workerID < workerCount; workerID++ {
		// process data in parallel
		go func(workerID int, data []byte) {
			last := workerSize*(workerID+1) + 20
			if last > len(data) {
				last = len(data) - 1
			}

			data = data[workerSize*workerID : last]
			data = data[bytes.IndexByte(data, '\n')+1 : bytes.LastIndexByte(data, '\n')+1]

			var (
				pos         int
				off         int
				stationID   uint64
				temperature int64
			)

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

				workerResults[workerID][stationID].count++
				workerResults[workerID][stationID].sum += temperature
				if temperature < workerResults[workerID][stationID].min {
					workerResults[workerID][stationID].min = temperature
				}
				if temperature > workerResults[workerID][stationID].max {
					workerResults[workerID][stationID].max = temperature
				}
			}

			done <- struct{}{}
		}(workerID, data)
	}

	// wait for all workers to finish
	for i := 0; i <= workerCount; i++ {
		<-done
	}

	// merge workerResults
	for _, result := range workerResults {
		for stationID, stationResult := range result {
			if stationResult.count == 0 {
				continue
			}

			stationResults[stationID].sum += stationResult.sum
			stationResults[stationID].count += stationResult.count
			if stationResult.min < stationResults[stationID].min {
				stationResults[stationID].min = stationResult.min
			}
			if stationResult.max > stationResults[stationID].max {
				stationResults[stationID].max = stationResult.max
			}
		}
	}

	var result stationResult

	buf := make([]byte, 0, 50000)
	buf = append(buf, '{')

	// Print workerResults {station1=min/avg/max, station2=min/avg/max, ...}
	for i, station := range stationNames {
		if i != 0 {
			buf = append(buf, ',', ' ')
		}

		result = stationResults[stationSymbolMap[maphash.Bytes(maphashSeed, station)]]

		buf = append(buf, station...)
		buf = append(buf, '=')
		buf = append(buf, strconv.FormatFloat(float64(result.min)/10, 'f', 1, 64)...)
		buf = append(buf, '/')
		buf = append(buf, strconv.FormatFloat(float64(result.sum)/(float64(result.count)*10), 'f', 1, 64)...)
		buf = append(buf, '/')
		buf = append(buf, strconv.FormatFloat(float64(result.max)/10, 'f', 1, 64)...)
	}

	buf = append(buf, '}', '\n')
	_, _ = os.Stdout.Write(buf)
}
