package main

import (
	json2 "encoding/json"
	"fmt"
)

var okResponse = newRawBytesResponse("+OK\r\n", false)
var pongResponse = newRawBytesResponse("+PONG\r\n", false)
var byeResponse = newRawBytesResponse("+BYE\r\n", true)

type response interface {
	write(sink *sink) (*ioData, error)
	isFinal() bool
}

type stringResponse struct {
	data []byte
}

func (s *stringResponse) write(sink *sink) (*ioData, error) {
	return sink.flushAsProtocolString(s.data)
}

func (s *stringResponse) isFinal() bool {
	return false
}

func newStringResponse(data []byte) response {
	return &stringResponse{data: data}
}

type rawBytesResponse struct {
	data  []byte
	final bool
}

func (s *rawBytesResponse) write(sink *sink) (*ioData, error) {
	return sink.flushWrite(s.data)
}

func (s *rawBytesResponse) isFinal() bool {
	return s.final
}

func newRawBytesResponse(data string, final bool) response {
	return &rawBytesResponse{data: []byte(data), final: final}
}

func newErrorResponse(message string, final bool) response {
	message = "-ERR " + message + "\r\n"
	return newRawBytesResponse(message, final)
}

func newJsonResponse(object any) response {
	json, err := json2.Marshal(object)
	if err != nil {
		// TODO: better error handling
		panic(err)
	}

	return newStringResponse(json)
}

type syncResponse struct {
	items []*item
}

func (s *syncResponse) write(sink *sink) (*ioData, error) {
	arrayHeader := fmt.Sprintf("*%v\r\n", len(s.items))
	sink.writeAsRawBytes(arrayHeader)
	total := newIoData()

	for _, item := range s.items {
		pieces := item.genSetCommandPieces()
		sink.writeArrayOfProtocolStrings(pieces...)
		if sink.full() {
			data, err := sink.flush()
			total.merge(data)
			if err != nil {
				return total, err
			}
		}
	}

	data, err := sink.flush()
	total.merge(data)
	return total, err
}

func (s *syncResponse) isFinal() bool {
	return false
}

func newSyncResponse(items []*item) response {
	return &syncResponse{items: items}
}

type broadcastCommandResponse struct {
	command *command
}

func (r *broadcastCommandResponse) write(sink *sink) (*ioData, error) {
	pieces := r.command.toPieces()
	return sink.flushArrayOfProtocolStrings(pieces...)
}

func (s *broadcastCommandResponse) isFinal() bool {
	return false
}

func newBroadcastCommandResponse(c *command) response {
	return &broadcastCommandResponse{command: c}
}
