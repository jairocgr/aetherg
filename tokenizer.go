package main

import "fmt"

type tokenType string
type tokenizerState string

const (
	tokenComment    tokenType = "COMMENT"
	tokenIdentifier tokenType = "IDENTIFIER"
	tokenString     tokenType = "STRING"
	tokenEol        tokenType = "EOL"
	tokenArray      tokenType = "ARRAY"
	tokenBinString  tokenType = "BIN_STRING"
)

const (
	tokenizerWaitingForToken           tokenizerState = "WAITING_FOR_TOKEN"
	tokenizerReadingIdentifier         tokenizerState = "READING_IDENTIFIER"
	tokenizerReadingString             tokenizerState = "READING_STRING"
	tokenizerWaitingSpaceForEos        tokenizerState = "WAITING_SPACE_FOR_EOS"
	tokenizerWaitingEol                tokenizerState = "WAITING_EOL"
	tokenizerReadingComment            tokenizerState = "READING_COMMENT"
	tokenizerReadingSize               tokenizerState = "READING_SIZE"
	tokenizerReadingBinStringSeparator tokenizerState = "READING_BIN_STRING_SEPARATOR"
	tokenizerWaitingEobs               tokenizerState = "WAITING_EOBS"
	tokenizerReadingBinString          tokenizerState = "READING_BIN_STRING"
	tokenizerScape                     tokenizerState = "SCAPE"
)

const (
	carryReturn = 13 // \r
	lineFeed    = 10 // \n
	doubleQuote = 34 // "
	singleQuote = 39 // '
	space       = 32
	tab         = 9
	numberSign  = 35  // # char
	dollar      = 36  // The dollar $ sign
	asterisk    = 42  // * char
	del         = 127 // del char
	scape       = 92  // The backslash char "\"
)

type token struct {
	ttype     tokenType
	data      []byte
	delimiter byte
	size      int
}

type tokenizer struct {
	src              *source
	state            tokenizerState
	token            *token
	lines            int
	allowedTokenSize int
}

func (tokenizer *tokenizer) getNumberOfReads() int {
	return tokenizer.src.numOfReads()
}

func (tokenizer *tokenizer) getBytesRead() int {
	return tokenizer.src.bytesIn()
}

func newTokenizer(src *source, allowedTokenSize int) *tokenizer {
	tokenizer := new(tokenizer)
	tokenizer.src = src
	tokenizer.state = tokenizerWaitingForToken
	tokenizer.allowedTokenSize = allowedTokenSize
	return tokenizer
}

func realPrintable(ch byte) bool {
	return ch > space && ch < del
}

const zeroChar = 48
const nineChar = 57

func asciiDigit(ch byte) bool {
	return ch >= zeroChar && ch <= nineChar
}

func ascii2int(ch byte) int {
	switch ch {
	case 48:
		return 0
	case 49:
		return 1
	case 50:
		return 2
	case 51:
		return 3
	case 52:
		return 4
	case 53:
		return 5
	case 54:
		return 6
	case 55:
		return 7
	case 56:
		return 8
	case 57:
		return 9
	default:
		panic(fmt.Errorf("Invalid ASCII '%v' conversion to int", ch))
	}
}

func (tokenizer *tokenizer) next() (*token, *ioData, *parsingError) {
	in := newIoData()
	for {
		ch, bytes, err := tokenizer.peek()
		in.add(bytes)
		if err != nil {
			return nil, in, newReadingError(err)
		}
		switch tokenizer.state {
		case tokenizerWaitingForToken:
			switch ch {
			case dollar:
				tokenizer.new(tokenBinString)
				tokenizer.consume()
				tokenizer.state = tokenizerReadingSize
			case asterisk:
				tokenizer.new(tokenArray)
				tokenizer.consume()
				tokenizer.state = tokenizerReadingSize
			case numberSign:
				tokenizer.new(tokenComment)
				tokenizer.consume()
				tokenizer.state = tokenizerReadingComment
			case space, tab:
				// Eat and ignore the space between tokens
				tokenizer.consume()
				tokenizer.state = tokenizerWaitingForToken
			case carryReturn:
				tokenizer.new(tokenEol)
				tokenizer.consume()
				tokenizer.state = tokenizerWaitingEol
			case lineFeed:
				tokenizer.new(tokenEol)
				tokenizer.consume()
				tokenizer.state = tokenizerWaitingForToken
				return tokenizer.yield(), in, nil
			case singleQuote, doubleQuote:
				tokenizer.new(tokenString)
				tokenizer.setDelimiter(ch)
				tokenizer.consume()
				tokenizer.state = tokenizerReadingString
			default:
				if realPrintable(ch) {
					tokenizer.new(tokenIdentifier)
					tokenizer.append(ch)
					tokenizer.consume()
					tokenizer.state = tokenizerReadingIdentifier
					break
				}
				return nil, in, tokenizer.illegalChar(ch)
			}
		case tokenizerReadingIdentifier:
			switch ch {
			case carryReturn, lineFeed, space, tab:
				tokenizer.state = tokenizerWaitingForToken
				return tokenizer.yield(), in, nil
			case doubleQuote, singleQuote:
				return nil, in, tokenizer.unexpectedQuote()
			default:
				if realPrintable(ch) {
					if tokenizer.reachedSizeLimit() {
						return nil, in, tokenizer.tokenTooBig()
					}
					tokenizer.append(ch)
					tokenizer.consume()
					tokenizer.state = tokenizerReadingIdentifier
					break
				}
				return nil, in, tokenizer.illegalChar(ch)
			}
		case tokenizerReadingString:
			switch ch {
			case carryReturn, lineFeed:
				return nil, in, tokenizer.unexpectedEol()
			case scape:
				if tokenizer.reachedSizeLimit() {
					return nil, in, tokenizer.tokenTooBig()
				}
				tokenizer.append(ch)
				tokenizer.consume()
				tokenizer.state = tokenizerScape
			case tokenizer.delimiter():
				// We've reached the end of the string
				tokenizer.consume()
				tokenizer.state = tokenizerWaitingSpaceForEos
				return tokenizer.yield(), in, nil
			default:
				// If we've got a doubleQuote char here it means that we are reading
				// a SINGLE_QUOTED string and we should store the " properly scaped
				if ch == doubleQuote {
					tokenizer.append(scape)
				}

				if tokenizer.reachedSizeLimit() {
					return nil, in, tokenizer.tokenTooBig()
				}

				// Whatever ch is, add to the string being read
				tokenizer.append(ch)
				tokenizer.consume()
				tokenizer.state = tokenizerReadingString
			}
		case tokenizerWaitingSpaceForEos:
			if ch == lineFeed || ch == carryReturn || ch == space {
				tokenizer.state = tokenizerWaitingForToken
			} else {
				return nil, in, tokenizer.unexpectedCharEos(ch)
			}
		case tokenizerWaitingEol:
			if ch != lineFeed {
				return nil, in, tokenizer.missingLineFeed()
			}
			tokenizer.consume()
			tokenizer.state = tokenizerWaitingForToken
			return tokenizer.yield(), in, nil
		case tokenizerReadingComment:
			if ch == carryReturn || ch == lineFeed {
				tokenizer.state = tokenizerWaitingForToken
				return tokenizer.yield(), in, nil
			} else {
				tokenizer.consume()
				tokenizer.state = tokenizerReadingComment
			}
		case tokenizerReadingSize:
			if asciiDigit(ch) {
				tokenizer.increaseSize(ch)
				tokenizer.consume()
				tokenizer.state = tokenizerReadingSize
			} else if ch == carryReturn || ch == lineFeed {
				if tokenizer.token.is(tokenArray) {
					tokenizer.state = tokenizerWaitingForToken
					return tokenizer.yield(), in, nil
				} else {
					if tokenizer.reachedSizeLimit() {
						return nil, in, tokenizer.tokenTooBig()
					}
					tokenizer.state = tokenizerReadingBinStringSeparator
				}
			} else {
				return nil, in, tokenizer.illegalChar(ch)
			}
		case tokenizerReadingBinStringSeparator:
			if ch == carryReturn {
				// Not counting the number of CR read, we will accept the odd
				// case of we have multiples CR before a LF
				//
				// Something like this will be accepted
				//  "$343\r\r\r\r\r\n{string_content...}\r\n..."
				//
				// TODO: Reconsider this case!
				//
				tokenizer.consume()
				tokenizer.state = tokenizerReadingBinStringSeparator
			} else if ch == lineFeed {
				tokenizer.consume()
				tokenizer.state = tokenizerReadingBinString
				tokenizer.lines += 1
			} else {
				return nil, in, tokenizer.illegalChar(ch)
			}
		case tokenizerWaitingEobs:
			if ch == carryReturn {
				// Not counting the number of CR read, we will accept the odd
				// case of we have multiples CR before a LF
				//
				// Something like this will be accepted
				//  "${size}\r\n{string_content}\r\r\r\n..."
				//
				// TODO: Reconsider this case!
				//
				tokenizer.consume()
				tokenizer.state = tokenizerWaitingEobs
			} else if ch == lineFeed {
				// We've reached the EOBS
				tokenizer.consume()
				tokenizer.state = tokenizerWaitingForToken
				tokenizer.lines += 1
				return tokenizer.yield(), in, nil
			} else {
				return nil, in, tokenizer.illegalChar(ch)
			}
		case tokenizerReadingBinString:
			if tokenizer.reachedExpectedSizeOfBinaryString() {
				tokenizer.state = tokenizerWaitingEobs
				break
			}
			tokenizer.append(ch)
			tokenizer.consume()
			tokenizer.state = tokenizerReadingBinString
		case tokenizerScape:
			if tokenizer.reachedSizeLimit() {
				return nil, in, tokenizer.tokenTooBig()
			}
			tokenizer.append(ch)
			tokenizer.consume()
			tokenizer.state = tokenizerReadingString
		default:
			// TODO: use fatal() or make this a event or error
			panic(fmt.Errorf("Invalid tokenizer state %v", tokenizer.state))
		}
	}
}

func (tokenizer *tokenizer) illegalChar(ch byte) *parsingError {
	return newTokenizationError("Illegal char %v", ch)
}

func (tokenizer *tokenizer) unexpectedCharEos(ch byte) *parsingError {
	return newTokenizationError("Unexpected '%v' next to EOS", ch)
}

func (tokenizer *tokenizer) unexpectedEol() *parsingError {
	return newTokenizationError("Unexpected end of line")
}

func (tokenizer *tokenizer) missingLineFeed() *parsingError {
	return newTokenizationError("Missing line feed after carry return to proper end a line")
}

func (tokenizer *tokenizer) unexpectedQuote() *parsingError {
	return newTokenizationError("Unexpected quote")
}

func (tokenizer *tokenizer) tokenTooBig() *parsingError {
	return newTokenizationError("Token is too big (max size allowed is %v bytes)", tokenizer.allowedTokenSize)
}

func (tokenizer *tokenizer) reachedSizeLimit() bool {
	return tokenizer.token.getSize() >= tokenizer.allowedTokenSize
}

func (tokenizer *tokenizer) new(ttype tokenType) {
	tokenizer.token = new(token)
	tokenizer.token.ttype = ttype
}

func (tokenizer *tokenizer) increaseSize(ch byte) {
	tokenizer.token.increaseSize(ch)
}

const stringSizeRadix = 10

func (token *token) increaseSize(ch byte) {
	value := ascii2int(ch)
	token.size = (token.size * stringSizeRadix) + value
}

func (tokenizer *tokenizer) consume() {
	tokenizer.src.rm()
}

func (tokenizer *tokenizer) peek() (ch byte, bytes int, err error) {
	return tokenizer.src.peek()
}

func (tokenizer *tokenizer) yield() *token {
	if tokenizer.token.is(tokenEol) {
		tokenizer.lines += 1
	}
	return tokenizer.token
}

func (tokenizer *tokenizer) append(ch byte) {
	tokenizer.token.append(ch)
}

func (tokenizer *tokenizer) delimiter() byte {
	return tokenizer.token.delimiter
}

func (tokenizer *tokenizer) setDelimiter(ch byte) {
	tokenizer.token.delimiter = ch
}

func (tokenizer *tokenizer) reachedExpectedSizeOfBinaryString() bool {
	return tokenizer.token.expectedSizeAndDataLenghtIsMatching()
}

func (token *token) is(ttype tokenType) bool {
	return token.ttype == ttype
}

func (token *token) expectedSizeAndDataLenghtIsMatching() bool {
	return len(token.data) == token.getSize()
}

func (token *token) append(ch byte) {
	token.data = append(token.data, ch)
	switch token.ttype {
	case tokenIdentifier, tokenString:
		token.size += 1
	}
}

func (token *token) getType() tokenType {
	return token.ttype
}

func (token *token) value() string {
	return string(token.data)
}

func (token *token) getData() []byte {
	return token.data
}

func (token *token) getSize() int {
	return token.size
}
