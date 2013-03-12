package main

import "time"
import "log"

var start time.Time

type StatsType struct {
  files int
  directories int
  bytes int64
  working int
  errors int
}

var Stats StatsType

func printStats() {
  log.Printf("%+v", Stats)
}

func statsWorker(period int) {
  start = time.Now()

  delay := time.Duration(period) * time.Second
  for {
    printStats()
    time.Sleep(delay)
  }
}
