package main

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func logError(message string, err error) {
	log.WithFields(log.Fields{"error": err}).Error(message)
}

func fatalError(message string, err error) {
	fatal(message, log.Fields{"error": err})
}

func fatal(message string, fields log.Fields) {
	log.WithFields(fields).Error(message)
	os.Exit(EXIT_FAILURE)
}

func info(message string, fields log.Fields) {
	log.WithFields(fields).Info(message)
}
