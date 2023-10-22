package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
)

const defaultSnapshotFile = "aetherg.snap"

func main() {
	settings := parseArgs()
	server := NewAetherServer(settings)
	server.Run()
}

func parseArgs() AetherSettings {
	var host string
	var port int
	var replicate bool
	var source string
	var loggingLevel string
	var json bool
	var snapshot string

	flag.StringVar(&host, "h", "localhost", "Server's tcp host")
	flag.IntVar(&port, "p", 3000, "Server's tcp port")
	flag.BoolVar(&replicate, "r", false, "Replication flag")
	flag.StringVar(&source, "s", "localhost:3000", "Server from which should be replicated")
	flag.StringVar(&loggingLevel, "l", "trace", "Logging level (trace, debug, info, etc)")
	flag.BoolVar(&json, "j", false, "JSON logger formatter")
	flag.StringVar(&snapshot, "f", defaultSnapshotFile, "Path to snapshot file")

	flag.Parse()

	if json {
		log.SetFormatter(&log.JSONFormatter{})
	}

	level, err := log.ParseLevel(loggingLevel)
	if err != nil {
		log.Fatal(err)
	}

	log.SetLevel(level) // Maybe this could be hardcoded instead...

	return AetherSettings{
		Port:          port,
		Host:          host,
		Replicate:     replicate,
		SourceAddress: source,
		Snapshot:      snapshot,
	}
}
