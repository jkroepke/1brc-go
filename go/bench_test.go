package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"
)

func BenchmarkShort(b *testing.B) {
	for i := 0; i < b.N; i++ {
		execute("../measurements100000000.txt")
	}

	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkReal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		execute("../measurements.txt")
	}

	b.StopTimer()
	b.ReportAllocs()
}

func BenchmarkPrint(b *testing.B) {
	result := stationResult{
		count: 10,
		min:   -340,
		max:   343,
		sum:   3800,
	}

	os.Stdout, _ = os.Open(os.DevNull)

	station := []byte("station")

	b.Run("fmt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			fmt.Printf("%s=%.1f/%.1f/%.1f",
				station,
				float64(result.min)/10,
				float64(result.sum)/(float64(result.count)*10),
				float64(result.max)/10,
			)
		}

		b.StopTimer()
		fmt.Print("\n")

		b.ReportAllocs()
	})

	b.Run("Write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = fmt.Fprint(os.Stdout, station)
			_, _ = fmt.Fprint(os.Stdout, "=")
			_, _ = fmt.Fprint(os.Stdout, strconv.FormatFloat(float64(result.min)/10, 'f', 1, 64))
			_, _ = fmt.Fprint(os.Stdout, "/")
			_, _ = fmt.Fprint(os.Stdout, strconv.FormatFloat(float64(result.sum)/(float64(result.count)*10), 'f', 1, 64))
			_, _ = fmt.Fprint(os.Stdout, "/")
			_, _ = fmt.Fprint(os.Stdout, strconv.FormatFloat(float64(result.max)/10, 'f', 1, 64))
		}

		b.StopTimer()
		fmt.Print("\n")

		b.ReportAllocs()
	})

	b.Run("Buffer", func(b *testing.B) {

		for i := 0; i < b.N; i++ {
			sb := bytes.Buffer{}
			sb.Write(station)
			sb.WriteRune('=')
			sb.WriteString(strconv.FormatFloat(float64(result.min)/10, 'f', 1, 64))
			sb.WriteRune('/')
			sb.WriteString(strconv.FormatFloat(float64(result.sum)/(float64(result.count)*10), 'f', 1, 64))
			sb.WriteRune('/')
			sb.WriteString(strconv.FormatFloat(float64(result.max)/10, 'f', 1, 64))

			os.Stdout.Write(sb.Bytes())
		}

		b.StopTimer()
		fmt.Print("\n")

		b.ReportAllocs()
	})

	b.Run("Slice", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 0, 1000)
			buf = append(buf, station...)
			buf = append(buf, '=')
			buf = append(buf, strconv.FormatFloat(float64(result.min)/10, 'f', 1, 64)...)
			buf = append(buf, '/')
			buf = append(buf, strconv.FormatFloat(float64(result.sum)/(float64(result.count)*10), 'f', 1, 64)...)
			buf = append(buf, '/')
			buf = append(buf, strconv.FormatFloat(float64(result.max)/10, 'f', 1, 64)...)
			_, _ = os.Stdout.Write(buf)
		}

		b.StopTimer()
		fmt.Print("\n")

		b.ReportAllocs()
	})
}
