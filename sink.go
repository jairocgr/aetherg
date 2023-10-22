package main

import (
	"fmt"
)

type outputStream interface {
	Write(b []byte) (n int, err error)
}

type sink struct {
	output    outputStream
	buffer    []byte
	threshold int
	writes    int
	written   int
}

func newSink(output outputStream, writeThreshold int) *sink {
	return &sink{
		output:    output,
		threshold: writeThreshold,
		buffer:    make([]byte, 0),
	}
}

func (s *sink) write(chunks ...[]byte) {
	for _, chunk := range chunks {
		s.buffer = append(s.buffer, chunk...)
	}
}

func (s *sink) flushWrite(chunks ...[]byte) (*ioData, error) {
	s.write(chunks...)

	data, err := s.flush()
	if err != nil {
		return data, err
	}

	return data, nil
}

func (s *sink) writeAsProtocolString(data []byte) {
	size := fmt.Sprintf("$%v\r\n", len(data))
	s.write([]byte(size), data, []byte("\r\n"))
}

func (s *sink) flushAsProtocolString(data []byte) (*ioData, error) {
	s.writeAsProtocolString(data)
	return s.flush()
}

func (s *sink) getBytesWritten() int {
	return s.written
}

func (s *sink) getNumberOfWrites() int {
	return s.writes
}

func (s *sink) writeArrayOfProtocolStrings(pieces ...[]byte) {
	arrayToken := bprintf("*%v\r\n", len(pieces))
	s.write(arrayToken)
	for _, chunk := range pieces {
		s.writeAsProtocolString(chunk)
	}
}

func (s *sink) flushArrayOfProtocolStrings(pieces ...[]byte) (*ioData, error) {
	s.writeArrayOfProtocolStrings(pieces...)
	return s.flush()
}

func (s *sink) writeAsRawBytes(str string) {
	s.write([]byte(str))
}

func (s *sink) flushAsRawBytes(str string) (*ioData, error) {
	s.writeAsRawBytes(str)
	return s.flush()
}

func (s *sink) flush() (*ioData, error) {
	if s.empty() {
		return newIoData(), nil
	}
	data := newIoData()
	count, err := s.output.Write(s.buffer)
	data.add(count)
	if err != nil {
		return data, err
	}
	defer s.resetBuffer()
	return data, nil
}

func (s *sink) full() bool {
	return len(s.buffer) >= s.threshold
}

func (s *sink) empty() bool {
	return len(s.buffer) == 0
}

func (s *sink) resetBuffer() {
	s.buffer = make([]byte, 0)
}
