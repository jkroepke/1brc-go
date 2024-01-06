package main

import (
	"bufio"
	"bytes"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/mmap"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fastjson/fastfloat"
)

const workerCount = 12

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

	stationNames := make(map[string]uint32, 10_000)
	stationMap := [10_000]stationResult{}

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		panic(err)
	}

	sneak := make([]byte, 1_000_000)
	_, err = readerAt.ReadAt(sneak, 0)
	if err != nil {
		panic(err)
	}
	sneakScanner := bufio.NewScanner(bytes.NewReader(sneak))

	for sneakScanner.Scan() {
		station, _, _ := strings.Cut(sneakScanner.Text(), ";")
		if _, ok := stationNames[station]; !ok {
			stationNames[station] = id
			id++

			stationMap[stationNames[station]] = stationResult{}
		}
	}

	workerSize := readerAt.Len() / workerCount

	stations := maps.Keys(stationNames)
	slices.Sort(stations)

	wgl := sync.WaitGroup{}

	results := [12][10_000]stationResult{}
	for i := 0; i < workerCount; i++ {
		wgl.Add(1)
		i := i

		go func() {
			defer wgl.Done()

			var tempF float64

			data := make([]byte, workerSize+20)
			_, _ = readerAt.ReadAt(data, int64(workerSize*i))

			scanner := bufio.NewScanner(bytes.NewReader(data))
			if i != 0 {
				scanner.Scan()
			}
			for scanner.Scan() {
				station, temp, _ := strings.Cut(scanner.Text(), ";")
				tempF = fastfloat.ParseBestEffort(temp)

				stationID := stationNames[station]

				results[i][stationID].num++
				results[i][stationID].sum += tempF
				results[i][stationID].min = math.Min(results[i][stationID].min, tempF)
				results[i][stationID].max = math.Max(results[i][stationID].max, tempF)
			}
		}()
	}

	wgl.Wait()

	for _, result := range results {
		for station, stationResult := range result {
			if stationResult.num == 0 {
				continue
			}

			stationMap[station].min = math.Min(stationMap[station].min, stationResult.min)
			stationMap[station].max = math.Max(stationMap[station].max, stationResult.max)
			stationMap[station].sum += stationResult.sum
			stationMap[station].num += stationResult.num
		}
	}

	result := stationMap[stationNames[stations[0]]]
	print("{",
		stations[0], "=",
		int(result.min), "/",
		int(result.sum/result.num), "/",
		int(result.max),
	)

	for _, station := range stations[1:] {
		result := stationMap[stationNames[station]]
		print(", ",
			station, "=",
			int(result.min), "/",
			int(result.sum/result.num), "/",
			int(result.max),
		)
	}

	print("}")
	print("\n")
}
