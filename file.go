package main

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, os.ErrNotExist)
}

func removeFile(file *os.File) {
	path := file.Name()

	if !fileExists(path) {
		return // File don't exist or was already removed
	}

	err := os.Remove(path)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"path":  path,
		}).Error("Error trying to delete a file")
		os.Exit(EXIT_FAILURE)
	}
}

func absPath(file string) string {
	path, err := filepath.Abs(file)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"file":  file,
		}).Error("Couldn't get absolute path to snapshot file")
		os.Exit(EXIT_FAILURE)
	}
	return path
}
