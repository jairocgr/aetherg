package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWrite(t *testing.T) {
	assert := assert.New(t)

	file := newMockOutputStream()

	sink := newSink(file, 10)
	assert.True(sink.empty())

	sink.write([]byte("GET"))
	assert.False(sink.full())

	sink.write([]byte("  "), []byte("\"KEY_NAME\""))
	assert.True(sink.full())
	data, err := sink.flush()
	assert.Nil(err)
	assert.Equal(1, data.getCalls())
	assert.Equal(15, data.getByteCount())

	sink.write([]byte("\r\n"))
	assert.False(sink.full())
	data, err = sink.flush()
	assert.Nil(err)
	assert.Equal(1, data.getCalls())
	assert.Equal(2, data.getByteCount())

	assert.Equal("GET  \"KEY_NAME\"\r\n", file.stringContent())
	assert.Equal(2, file.writeCount())
}

func TestWriteArrayOfProtocolStrings(t *testing.T) {
	assert := assert.New(t)

	file := newMockOutputStream()

	sink := newSink(file, 64)
	assert.True(sink.empty())

	sink.writeArrayOfProtocolStrings([]byte("SET"), []byte("key0"), []byte("value-zero"))
	assert.False(sink.full())

	sink.writeArrayOfProtocolStrings([]byte("SET"), []byte("key1"), []byte("value-one"))
	assert.True(sink.full())
	data, err := sink.flush()
	assert.Nil(err)
	assert.True(sink.empty())
	assert.Equal(1, data.getCalls())
	assert.Equal(78, data.getByteCount())

	sink.writeArrayOfProtocolStrings([]byte("RM"), []byte("key3"))
	assert.False(sink.full())
	data, err = sink.flush()
	assert.Nil(err)
	assert.True(sink.empty())
	assert.Equal(1, data.getCalls())
	assert.Equal(22, data.getByteCount())

	expectedContent :=
		"*3\r\n$3\r\nSET\r\n$4\r\nkey0\r\n$10\r\nvalue-zero\r\n" + // 40 bytes
			"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$9\r\nvalue-one\r\n" + // 38 bytes
			"*2\r\n$2\r\nRM\r\n$4\r\nkey3\r\n" // 22 bytes

	assert.Equal(expectedContent, file.stringContent())
	assert.Equal(2, file.writeCount())
}

type mockOutputStream struct {
	writes [][]byte
}

func newMockOutputStream() *mockOutputStream {
	return &mockOutputStream{writes: make([][]byte, 0)}
}

func (mock *mockOutputStream) Write(data []byte) (n int, err error) {
	mock.writes = append(mock.writes, data)
	return len(data), nil
}

func (mock *mockOutputStream) stringContent() string {
	return string(mock.getContent())
}

func (mock *mockOutputStream) writeCount() int {
	return len(mock.writes)
}

func (mock *mockOutputStream) getContent() []byte {
	content := make([]byte, 0)

	for _, chunk := range mock.writes {
		content = append(content, chunk...)
	}

	return content
}
