package main

import (
	"fmt"
	"io"
)

type parsingError struct {
	message   string
	fatal     bool
	technical bool
	cause     error
}

func (err parsingError) Error() string {
	if err.hasCause() {
		return fmt.Sprintf("%s (cause: %s)", err.getMessage(), err.cause.Error())
	} else {
		return err.getMessage()
	}
}

func (err parsingError) getMessage() string {
	return err.message
}

func (err parsingError) isFatal() bool {
	return err.fatal
}

func (err parsingError) isTechnical() bool {
	return err.technical
}

func (err parsingError) getCause() error {
	return err.cause
}

func (err parsingError) hasCause() bool {
	return err.cause != nil
}

func (err parsingError) toErrorResponse() response {
	return newErrorResponse(err.message, err.isFatal())
}

func (err parsingError) isEOF() bool {
	return err.cause == io.EOF
}

func newParsingError(message string, a ...any) *parsingError {
	return &parsingError{
		message:   fmt.Sprintf(message, a...),
		fatal:     false,
		technical: false,
	}
}

func newTokenizationError(message string, a ...any) *parsingError {
	return &parsingError{
		message:   fmt.Sprintf(message, a...),
		fatal:     true,
		technical: false,
	}
}

func newReadingError(err error) *parsingError {
	return &parsingError{
		message:   "error reading from master",
		fatal:     true,
		technical: true,
		cause:     err,
	}
}
