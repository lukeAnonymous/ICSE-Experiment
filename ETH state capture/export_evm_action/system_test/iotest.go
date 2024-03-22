package main

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/disk"
)

func main() {
	go func() {
		var (
			lastReadCount  uint64 = 0
			lastWriteCount uint64 = 0
		)

		for {
			counters, err := disk.IOCounters()
			if err == nil {
				for _, counter := range counters {
					//fmt.Println(counter.Name)
					if counter.Name == "C:" {

						fmt.Printf("Disk I/O operations per second: read:%d write:%d\n", counter.ReadCount-lastReadCount, counter.WriteCount-lastWriteCount)
						lastReadCount = counter.ReadCount
						lastWriteCount = counter.WriteCount
					}
				}
			}
			time.Sleep(time.Second)
		}
	}()

	// do some other work here in the main thread
	for {
		time.Sleep(time.Second)
	}
}
