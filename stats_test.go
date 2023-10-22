package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAnalyzeIoMetrics(t *testing.T) {
	assert := assert.New(t)
	stats := newIoStatistics()
	dataPoints := []event{
		newDiskWriteEvent(&ioData{bytes: 1024, calls: 1}),
		newNetworkReadEvent(&ioData{bytes: 241, calls: 2}),
		newNetworkReadEvent(&ioData{bytes: 49, calls: 1}),
		newNetworkWriteEvent(&ioData{bytes: 300, calls: 1}),
		newNetworkWriteEvent(&ioData{bytes: 4, calls: 1}),
		newNetworkReadEvent(&ioData{bytes: 12, calls: 1}),
		newDiskWriteEvent(&ioData{bytes: 1024, calls: 1}),
		newNetworkReadEvent(&ioData{bytes: 16, calls: 1}),
	}

	for _, data := range dataPoints {
		event := data.(*ioEvent)
		stats.add(event)
	}

	net, disk := stats.analyze()

	assert.Equal(318, net.In)
	assert.Equal(304, net.Out)
	assert.Equal(5, net.Reads)
	assert.Equal(2, net.Writes)

	assert.Equal(0, disk.In)
	assert.Equal(2048, disk.Out)
	assert.Equal(0, disk.Reads)
	assert.Equal(2, disk.Writes)
}

func TestAnalyzeIoMetricsOverTime(t *testing.T) {
	assert := assert.New(t)
	stats := newIoStatistics()
	dataPoints := []event{
		newDiskWriteEvent(&ioData{bytes: 1024, calls: 1}),
		newNetworkReadEvent(&ioData{bytes: 241, calls: 2}),
		newNetworkReadEvent(&ioData{bytes: 49, calls: 1}),
		newNetworkWriteEvent(&ioData{bytes: 300, calls: 1}),
		newNetworkWriteEvent(&ioData{bytes: 4, calls: 1}),
		newNetworkReadEvent(&ioData{bytes: 12, calls: 1}),
		newDiskWriteEvent(&ioData{bytes: 1024, calls: 1}),
		newNetworkReadEvent(&ioData{bytes: 16, calls: 1}),
	}

	for _, data := range dataPoints {
		event := data.(*ioEvent)
		stats.add(event)
	}

	time.Sleep(2 * time.Second) // TODO: this should be mocked

	net, disk := stats.analyze()

	assert.Equal(318/2, net.In)
	assert.Equal(304/2, net.Out)
	assert.Equal(5/2, net.Reads)
	assert.Equal(2/2, net.Writes)

	assert.Equal(0/2, disk.In)
	assert.Equal(2048/2, disk.Out)
	assert.Equal(0/2, disk.Reads)
	assert.Equal(2/2, disk.Writes)
}
