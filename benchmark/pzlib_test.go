package pzlib_test

import (
	"bytes"
	"compress/zlib"
	"fmt"
	zlib2 "github.com/klauspost/compress/zlib"
	"github.com/zianke/pzlib"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"
)

func generateJson(size int) []byte {
	dat, _ := ioutil.ReadFile("testdata/test.json")
	dl := len(dat)
	testbuf := make([]byte, size)
	for j := 0; j < size; j += dl {
		if j+dl < size {
			copy(testbuf[j:j+dl], dat)
		} else {
			copy(testbuf[j:], dat[:size-j])
		}
	}
	return testbuf
}

func generateText(size int) []byte {
	dat, _ := ioutil.ReadFile("testdata/hamlet.txt")
	dl := len(dat)
	testbuf := make([]byte, size)
	for j := 0; j < size; j += dl {
		if j+dl < size {
			copy(testbuf[j:j+dl], dat)
		} else {
			copy(testbuf[j:], dat[:size-j])
		}
	}
	return testbuf
}

func generateRandom(size int) []byte {
	rand.Seed(15618)
	testbuf := make([]byte, size)
	for idx := range testbuf {
		testbuf[idx] = byte(65 + rand.Intn(32))
	}
	return testbuf
}

func generateDuplicate(size int) []byte {
	testbuf := make([]byte, size)
	for idx := range testbuf {
		testbuf[idx] = byte(65)
	}
	return testbuf
}

func benchmarkGeneral(i int, b *testing.B) {
	testbuf := generateJson(i)
	b.Run("zlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := zlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})

	b.Run("klauspost/compress/zlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := zlib2.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})

	b.Run("pzlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := pzlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})
}

func BenchmarkGeneral1K(b *testing.B)   { benchmarkGeneral(1000, b) }
func BenchmarkGeneral10K(b *testing.B)  { benchmarkGeneral(10000, b) }
func BenchmarkGeneral100K(b *testing.B) { benchmarkGeneral(100000, b) }
func BenchmarkGeneral1M(b *testing.B)   { benchmarkGeneral(1000000, b) }
func BenchmarkGeneral10M(b *testing.B)  { benchmarkGeneral(10000000, b) }
func BenchmarkGeneral100M(b *testing.B) { benchmarkGeneral(100000000, b) }
func BenchmarkGeneral1G(b *testing.B)   { benchmarkGeneral(1000000000, b) }

func BenchmarkFileSize(b *testing.B) {
	for _, fileSize := range []int{1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000} {
		testbuf := generateJson(fileSize)

		b.Run(fmt.Sprintf("zlib-%d", fileSize), func(b *testing.B) {
			b.SetBytes(int64(len(testbuf)))
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				br := bytes.NewBuffer(testbuf)
				var buf bytes.Buffer
				w, _ := zlib.NewWriterLevel(&buf, 6)
				io.Copy(w, br)
				w.Close()
			}
		})

		for _, threadNum := range []int{1, 2, 4, 8, 16, 32, 64} {
			b.Run(fmt.Sprintf("pzlib-%d-%d", fileSize, threadNum), func(b *testing.B) {
				b.SetBytes(int64(len(testbuf)))
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					br := bytes.NewBuffer(testbuf)
					var buf bytes.Buffer
					w, _ := pzlib.NewWriterLevel(&buf, 6)
					w.SetConcurrency(1<<20, threadNum)
					io.Copy(w, br)
					w.Close()
				}
			})
		}
	}
}

func BenchmarkBlockSize(b *testing.B) {
	testbuf := generateJson(100000000)

	b.Run("zlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := zlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})

	for _, blockSize := range []int{1 << 15, 1 << 16, 1 << 18, 1 << 20, 1 << 22, 1 << 24} {
		for _, threadNum := range []int{1, 2, 4, 8, 16, 32, 64} {
			b.Run(fmt.Sprintf("pzlib-%d-%d", blockSize, threadNum), func(b *testing.B) {
				b.SetBytes(int64(len(testbuf)))
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					br := bytes.NewBuffer(testbuf)
					var buf bytes.Buffer
					w, _ := pzlib.NewWriterLevel(&buf, 6)
					w.SetConcurrency(blockSize, threadNum)
					io.Copy(w, br)
					w.Close()
				}
			})
		}
	}
}

func benchmarkLevel(level int, b *testing.B) {
	testbuf := generateJson(100000000)

	b.SetBytes(int64(len(testbuf)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		br := bytes.NewBuffer(testbuf)
		var buf bytes.Buffer
		w, _ := pzlib.NewWriterLevel(&buf, level)
		io.Copy(w, br)
		w.Close()
	}
}

func BenchmarkLevel1(b *testing.B) { benchmarkLevel(1, b) }
func BenchmarkLevel2(b *testing.B) { benchmarkLevel(2, b) }
func BenchmarkLevel3(b *testing.B) { benchmarkLevel(3, b) }
func BenchmarkLevel4(b *testing.B) { benchmarkLevel(4, b) }
func BenchmarkLevel5(b *testing.B) { benchmarkLevel(5, b) }
func BenchmarkLevel6(b *testing.B) { benchmarkLevel(6, b) }
func BenchmarkLevel7(b *testing.B) { benchmarkLevel(7, b) }
func BenchmarkLevel8(b *testing.B) { benchmarkLevel(8, b) }
func BenchmarkLevel9(b *testing.B) { benchmarkLevel(9, b) }

func testLevel(level int, t *testing.T) {
	testbuf := generateJson(100000000)

	br := bytes.NewBuffer(testbuf)
	var buf bytes.Buffer
	w, _ := pzlib.NewWriterLevel(&buf, level)
	io.Copy(w, br)
	w.Close()

	log.Printf("[Level %d] Original size: %d bytes; Size after compression: %d bytes", level, len(testbuf), len(buf.Bytes()))

	testbuf = generateJson(100000000)

	br = bytes.NewBuffer(testbuf)
	var buf2 bytes.Buffer
	w2, _ := zlib.NewWriterLevel(&buf2, level)
	io.Copy(w2, br)
	w2.Close()

	log.Printf("[Level %d zlib] Original size: %d bytes; Size after compression: %d bytes", level, len(testbuf), len(buf2.Bytes()))
}

func TestLevel1(t *testing.T) { testLevel(1, t) }
func TestLevel2(t *testing.T) { testLevel(2, t) }
func TestLevel3(t *testing.T) { testLevel(3, t) }
func TestLevel4(t *testing.T) { testLevel(4, t) }
func TestLevel5(t *testing.T) { testLevel(5, t) }
func TestLevel6(t *testing.T) { testLevel(6, t) }
func TestLevel7(t *testing.T) { testLevel(7, t) }
func TestLevel8(t *testing.T) { testLevel(8, t) }
func TestLevel9(t *testing.T) { testLevel(9, t) }

func BenchmarkDuplicate(b *testing.B) {
	testbuf := generateDuplicate(100000000)

	b.Run("zlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := zlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})

	b.Run("pzlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := pzlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})
}

func TestDuplicate(t *testing.T) {
	testbuf := generateDuplicate(100000000)

	br1 := bytes.NewBuffer(testbuf)
	var buf1 bytes.Buffer
	w1, _ := zlib.NewWriterLevel(&buf1, 6)
	io.Copy(w1, br1)
	w1.Close()

	br2 := bytes.NewBuffer(testbuf)
	var buf2 bytes.Buffer
	w2, _ := pzlib.NewWriterLevel(&buf2, 6)
	io.Copy(w2, br2)
	w2.Close()

	log.Printf("[Duplicate] Original size: %d bytes; Size after compression by zlib: %d bytes; Size after compression by pzlib: %d bytes", len(testbuf), len(buf1.Bytes()), len(buf2.Bytes()))
}

func BenchmarkRandom(b *testing.B) {
	testbuf := generateRandom(100000000)

	b.Run("zlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := zlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})

	b.Run("pzlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := pzlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})
}

func TestRandom(t *testing.T) {
	testbuf := generateRandom(100000000)

	br1 := bytes.NewBuffer(testbuf)
	var buf1 bytes.Buffer
	w1, _ := zlib.NewWriterLevel(&buf1, 6)
	io.Copy(w1, br1)
	w1.Close()

	br2 := bytes.NewBuffer(testbuf)
	var buf2 bytes.Buffer
	w2, _ := pzlib.NewWriterLevel(&buf2, 6)
	io.Copy(w2, br2)
	w2.Close()

	log.Printf("[Random] Original size: %d bytes; Size after compression by zlib: %d bytes; Size after compression by pzlib: %d bytes", len(testbuf), len(buf1.Bytes()), len(buf2.Bytes()))
}

func BenchmarkJson(b *testing.B) {
	testbuf := generateJson(100000000)

	b.Run("zlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := zlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})

	b.Run("pzlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := pzlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})
}

func TestJson(t *testing.T) {
	testbuf := generateJson(100000000)

	br1 := bytes.NewBuffer(testbuf)
	var buf1 bytes.Buffer
	w1, _ := zlib.NewWriterLevel(&buf1, 6)
	io.Copy(w1, br1)
	w1.Close()

	br2 := bytes.NewBuffer(testbuf)
	var buf2 bytes.Buffer
	w2, _ := pzlib.NewWriterLevel(&buf2, 6)
	io.Copy(w2, br2)
	w2.Close()

	log.Printf("[JSON] Original size: %d bytes; Size after compression by zlib: %d bytes; Size after compression by pzlib: %d bytes", len(testbuf), len(buf1.Bytes()), len(buf2.Bytes()))
}

func BenchmarkText(b *testing.B) {
	testbuf := generateText(100000000)

	b.Run("zlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := zlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})

	b.Run("pzlib", func(b *testing.B) {
		b.SetBytes(int64(len(testbuf)))
		for n := 0; n < b.N; n++ {
			br := bytes.NewBuffer(testbuf)
			var buf bytes.Buffer
			w, _ := pzlib.NewWriterLevel(&buf, 6)
			io.Copy(w, br)
			w.Close()
		}
	})
}

func TestText(t *testing.T) {
	testbuf := generateText(100000000)

	br1 := bytes.NewBuffer(testbuf)
	var buf1 bytes.Buffer
	w1, _ := zlib.NewWriterLevel(&buf1, 6)
	io.Copy(w1, br1)
	w1.Close()

	br2 := bytes.NewBuffer(testbuf)
	var buf2 bytes.Buffer
	w2, _ := pzlib.NewWriterLevel(&buf2, 6)
	io.Copy(w2, br2)
	w2.Close()

	log.Printf("[Text] Original size: %d bytes; Size after compression by zlib: %d bytes; Size after compression by pzlib: %d bytes", len(testbuf), len(buf1.Bytes()), len(buf2.Bytes()))
}
