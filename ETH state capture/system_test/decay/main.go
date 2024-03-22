package main

import (
	"fmt"
	fifo "github.com/ethereum/go-ethereum/fifo"
	"math"
)

func main() {

	FIFOtest()
}
func DecayEquation(kvalue float64, index int, window_max_size int) float64 {
	a := -math.Log(kvalue) / float64(window_max_size-1)
	b := math.Log(1) * float64(window_max_size-1) / math.Log(kvalue)

	return math.Exp(-a * (float64(index) + b))
}
func FIFOtest() {
	fifo_windows := fifo.NewFIFO(5)
	fifo_windows.Enqueue("1.")
	fifo_windows.Enqueue("2.")
	fifo_windows.Enqueue("3.")
	fifo_windows.Enqueue("4.")
	fifo_windows.Enqueue("5.")
	fifo_windows.Enqueue("6.")

	fifo_windows.TraverseReverse(func(i interface{}, i2 int) {
		fmt.Println(i, i2)
		fmt.Println(DecayEquation(0.5, i2, fifo_windows.GetSize()))
	})
}
