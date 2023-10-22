package main

import (
	"time"
)

type ioType string
type ioDevice string

const input ioType = "INPUT"
const output ioType = "OUTPUT"

const disk ioDevice = "DISK"
const network ioDevice = "NETWORK"

type ioData struct {
	bytes int
	calls int
}

func (i *ioData) add(bytes int) {
	i.bytes += bytes
	if bytes > 0 {
		i.calls++
	}
}

func (i *ioData) merge(in *ioData) {
	i.bytes += in.bytes
	i.calls += in.calls
}

func (i *ioData) getCalls() int {
	return i.calls
}

func (i *ioData) getByteCount() int {
	return i.bytes
}

func newIoData() *ioData {
	return &ioData{}
}

type ioStatistics struct {
	network         ioStats
	disk            ioStats
	lastMeasurement time.Time
}

func (s *ioStatistics) add(event *ioEvent) {
	switch event.device {
	case network:
		s.network.add(event)
	case disk:
		s.disk.add(event)
	}
}

func (s *ioStatistics) analyze() (net ioStats, disk ioStats) {
	now := time.Now()
	elapsed := s.secondsElapsed(now)
	elapsed = max(1, elapsed) // at least 1sec or will divide by zero
	s.disk.average(elapsed)
	s.network.average(elapsed)
	defer s.reset(now)
	return s.network, s.disk
}

func (s *ioStatistics) reset(time time.Time) {
	s.lastMeasurement = time
	s.network = ioStats{}
	s.disk = ioStats{}
}

func (s *ioStatistics) secondsElapsed(now time.Time) int {
	elapsed := now.Sub(s.lastMeasurement)
	secs := elapsed.Seconds()
	return int(secs)
}
func newIoStatistics() *ioStatistics {
	return &ioStatistics{lastMeasurement: time.Now()}
}
