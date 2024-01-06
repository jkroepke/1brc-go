package main

import (
	"bufio"
	"bytes"
	maps "golang.org/x/exp/maps"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type stationResult struct {
	min float64
	max float64
	num float64
	sum float64
}

func main() {
	start := time.Now()
	execute(os.Args[1])

	diff := time.Since(start)
	print(diff.Milliseconds())
	println("ms")
}

func execute(fileName string) {
	var id uint32

	workerCount := int64(runtime.NumCPU())

	stationNames := make(map[[128]byte]uint32, 10_000)
	stationMap := make(map[uint32]stationResult, 10_000)

	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(file)
	var pos int

	for scanner.Scan() {
		var buf [128]byte
		station, _, _ := strings.Cut(scanner.Text(), ";")
		copy(buf[:], station)

		if _, ok := stationNames[buf]; !ok {
			stationNames[buf] = id
			id++

			stationMap[stationNames[buf]] = stationResult{}
		}

		pos += len(scanner.Bytes())
		if pos > 1_000_000_000 {
			break
		}
	}

	stat, _ := file.Stat()
	workerSize := stat.Size() / workerCount

	_ = file.Close()

	stations := maps.Keys(stationNames)
	sort.Slice(stations, func(i, j int) bool {
		return bytes.Compare(stations[i][:], stations[j][:]) < 0
	})

	wgl := sync.WaitGroup{}

	for i := int64(0); i < workerCount; i++ {
		wgl.Add(1)
		i := i

		go func() {
			defer wgl.Done()

			var tempF float64

			localMap := make(map[uint32]stationResult, 10_000)

			file, _ := os.Open(fileName)
			_, _ = file.Seek(workerSize*i, 0)

			scanner := bufio.NewScanner(file)
			if i != 0 {
				scanner.Scan()
			}

			var (
				ok     bool
				pos    int
				result stationResult
			)

			fileRange := int(workerSize + 20)
			for scanner.Scan() {
				var buf [128]byte
				station, temp, _ := strings.Cut(scanner.Text(), ";")
				copy(buf[:], station)

				tempF, _ = strconv.ParseFloat(temp, 64)

				if result, ok = localMap[stationNames[buf]]; !ok {
					result = stationResult{}
				}

				result.min = math.Min(result.min, tempF)
				result.max = math.Max(result.max, tempF)
				result.sum += tempF
				result.num++
				localMap[stationNames[buf]] = result

				pos += len(scanner.Bytes())
				if pos > fileRange {
					break
				}
			}
		}()
	}

	wgl.Wait()
	/*
		result, _ := stationMap.Load(stationNames[stations[0]])
		print("{",
			stations[0], "=",
			int(result.min), "/",
			int(result.sum/result.num), "/",
			int(result.max),
		)

		for _, station := range stations[1:] {
			result, _ := stationMap.Load(stationNames[station])
			print(", ",
				station, "=",
				int(result.min), "/",
				int(result.sum/result.num), "/",
				int(result.max),
			)
		}

		print("}")
		print("\n")
	*/
}
