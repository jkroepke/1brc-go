package main

import "testing"

func BenchmarkExecute(b *testing.B) {
	for i := 0; i < b.N; i++ {
		execute("../measurements100000000.txt")
	}

	b.StopTimer()
	b.ReportAllocs()
}
