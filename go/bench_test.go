package main

import (
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
