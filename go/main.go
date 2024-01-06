package main

import (
	"bufio"
	"bytes"
	"fmt"
	"golang.org/x/exp/mmap"
	"math"
	"os"
	"slices"
	"sync"
	"time"
)

const workerCount = 12

var results = [12][10_000]stationResult{}

type stationResult struct {
	count int64
	min   float64
	max   float64
	sum   float64
}

func main() {
	start := time.Now()

	execute(os.Args[1])

	_, _ = fmt.Fprintf(os.Stderr, "%dms\n", time.Since(start).Microseconds())
}

func execute(fileName string) {
	stationNames := make([]string, 0, 10_000)
	stationNameMap := make(map[string]int16, 10_000)
	stationResultMap := [10_000]stationResult{}

	file, err := mmap.Open(fileName)
	if err != nil {
		panic(err)
	}

	data := make([]byte, 1_000_000)
	_, _ = file.ReadAt(data, 0)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 32), bufio.MaxScanTokenSize)

	var id int16
	for scanner.Scan() {
		stationBytes, _, _ := bytes.Cut(scanner.Bytes(), []byte(";"))
		station := string(stationBytes)
		if _, ok := stationNameMap[station]; !ok {
			stationNames = append(stationNames, station)
			stationNameMap[station] = id
			stationResultMap[id] = stationResult{}
			id++
		}
	}

	workerSize := file.Len() / workerCount

	wg := sync.WaitGroup{}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			var (
				station           []byte
				stationID         int16
				temperature       float64
				temperatureString []byte
			)

			data := make([]byte, workerSize+20)
			_, _ = file.ReadAt(data, int64(workerSize*i))
			data = data[:bytes.LastIndexByte(data, '\n')]

			scanner := bufio.NewScanner(bytes.NewReader(data))
			if i != 0 {
				scanner.Scan()
			}

			for scanner.Scan() {
				station, temperatureString, _ = bytes.Cut(scanner.Bytes(), []byte(";"))
				temperature = parseFloat32(temperatureString)
				stationID = stationNameMap[string(station)]

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

	slices.Sort(stationNames)

	print("{")

	for i, station := range stationNames {
		if i != 0 {
			print(", ")
		}

		result := stationResultMap[stationNameMap[station]]
		fmt.Printf("%s=%.1f/%.1f/%.1f", station, round(float64(result.min)/10.0), round(float64(result.sum)/10.0/float64(result.count)), round(float64(result.max)/10.0))
	}

	print("}")
	print("\n")
}

func parseFloat32(float []byte) float64 {
	var (
		sign  float64 = 1
		tens  float64 = 0
		ones  float64 = 0
		comma float64 = 0
	)

	if float[0] == '-' {
		sign = -1
		float = float[1:]
	}

	switch len(float) {
	case 3:
		ones = float64(float[0] - 48)
		comma = float64(float[2] - 48)
	case 4:
		tens = float64(float[0] - 48)
		ones = float64(float[1] - 48)
		comma = float64(float[3] - 48)
	}

	if comma == 0 {
		return sign * (tens*10 + ones)
	}

	return sign * (tens*10 + ones + comma/10)
}

func round(x float64) float64 {
	return roundJava(x*10.0) / 10.0
}

// roundJava returns the closest integer to the argument, with ties
// rounding to positive infinity, see java's Math.round
func roundJava(x float64) float64 {
	t := math.Trunc(x)
	if x < 0.0 && t-x == 0.5 {
		//return t
	} else if math.Abs(x-t) >= 0.5 {
		t += math.Copysign(1, x)
	}

	if t == 0 { // check -0
		return 0.0
	}
	return t
}
