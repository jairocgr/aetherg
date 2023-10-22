package main

import (
	"io"
	"testing"
	"testing/fstest"
)

type tokenTypeValuePair struct {
	ttype tokenType
	value string
	size  int
	data  *ioData
}

const buffSize = 8

func TestBasicTokenizationOfAInput(t *testing.T) {
	fileContent := "GET F398BC5672A51D8D \n"
	expectedTokens := []tokenTypeValuePair{
		{tokenIdentifier, "GET", 3, &ioData{bytes: buffSize, calls: 1}},
		{tokenIdentifier, "F398BC5672A51D8D", 16, &ioData{bytes: 14, calls: 2}},
		{tokenEol, "", 0, &ioData{bytes: 0, calls: 0}},
	}
	runTokenizerTest(fileContent, expectedTokens, t)
}

func TestTokenizationOfBasicWiredProtocol(t *testing.T) {
	fileContent := "\t  *\n*2\r\n$3\r\nGET\r\n$16\nF398BC5672A51D8D\r\n\n"
	expectedTokens := []tokenTypeValuePair{
		{tokenArray, "", 0, &ioData{bytes: buffSize, calls: 1}},
		{tokenEol, "", 0, &ioData{bytes: 0, calls: 0}},
		{tokenArray, "", 2, &ioData{bytes: 0, calls: 0}},
		{tokenEol, "", 0, &ioData{bytes: buffSize, calls: 1}},
		{tokenBinString, "GET", 3, &ioData{bytes: 8, calls: 1}},
		{tokenBinString, "F398BC5672A51D8D", buffSize * 2, &ioData{bytes: 16, calls: 2}},
		{tokenEol, "", 0, &ioData{bytes: 1, calls: 1}},
	}
	runTokenizerTest(fileContent, expectedTokens, t)
}

func runTokenizerTest(fileContent string, expectedTokens []tokenTypeValuePair, t *testing.T) {
	fs := fstest.MapFS{
		"test/tokenizer/test.txt": {
			Data: []byte(fileContent),
		},
	}

	inputFile, err := fs.Open("test/tokenizer/test.txt")
	if err != nil {
		panic(err)
	}

	defer inputFile.Close()

	src := newBufferedSource(inputFile, buffSize)
	tokenizer := newTokenizer(src, 128)

	for i, expected := range expectedTokens {
		token, data, err := tokenizer.next()
		if err != nil {
			panic(err)
		}
		if token.getType() != expected.ttype {
			t.Errorf("Token #%v %v not typed as %v", i+1, token.getType(), expected.ttype)
		}
		if token.value() != expected.value {
			t.Errorf("Token #%v content %v not as expected %v", i+1, token.value(), expected.value)
		}
		if token.getSize() != expected.size {
			t.Errorf("Token #%v size %v not as expected %v", i+1, token.getSize(), expected.size)
		}
		if data.calls != expected.data.calls {
			t.Errorf("Token #%v read calls %v not as expected %v", i+1, data.calls, expected.data.calls)
		}
		if data.bytes != expected.data.bytes {
			t.Errorf("Token #%v bytes read %v not as expected %v", i+1, data.bytes, expected.data.bytes)
		}
	}

	_, _, parsingErr := tokenizer.next()
	if parsingErr.getCause() != io.EOF {
		t.Errorf("Expected EOF")
	}
}
