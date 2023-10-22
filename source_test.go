package main

import (
	"io"
	"math"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
)

func TestBufferedReadFromSource(t *testing.T) {
	runSourceTest("GET F398BC5672A51D8D \n", t)
}

func TestBufferedReadFromSingleByteSource(t *testing.T) {
	runSourceTest("S", t)
}

func TestBufferedReadFromEmptySource(t *testing.T) {
	runSourceTest("", t)
}

func runSourceTest(fileContent string, t *testing.T) {
	assert := assert.New(t)

	fs := fstest.MapFS{
		"test/source/test.txt": {
			Data: []byte(fileContent),
		},
	}

	basicInput, err := fs.Open("test/source/test.txt")
	if err != nil {
		panic(err)
	}

	defer basicInput.Close()

	const buffSize = 8
	src := newBufferedSource(basicInput, buffSize)
	expectedInput := []byte(fileContent)

	for i, expected := range expectedInput {
		ch, _, err := src.peek()
		if err != nil {
			panic(err)
		}
		if ch != expected {
			t.Errorf("Char read no #%v %v not equal to expected %v", i, ch, expected)
		}
		src.rm()
	}

	_, _, err = src.peek()
	if err != io.EOF {
		t.Errorf("Expected EOF")
	}

	expectedReads := int(math.Ceil(float64(len(expectedInput)) / buffSize))

	assert.Equal(src.numOfReads(), expectedReads, "Incorrect num of read calls")
	assert.Equal(src.bytesIn(), len(expectedInput), "Incorrect num of bytes read")
}
