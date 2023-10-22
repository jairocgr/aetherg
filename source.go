package main

type inputStream interface {
	Read(buff []byte) (n int, err error)
}

type source struct {
	fd    inputStream
	buff  []byte
	pos   int
	size  int
	reads int
	in    int
}

func newBufferedSource(fd inputStream, buffSize int) *source {
	src := new(source)
	src.fd = fd
	src.buff = make([]byte, buffSize)
	src.pos = 0
	src.size = 0
	return src
}

func (src *source) peek() (byte, int, error) {
	return src.readChar(false)
}

func (src *source) readChar(remove bool) (ch byte, bytes int, err error) {
	if src.hasUnreadInput() {
		if remove {
			return src.removeByte(), 0, nil
		} else {
			return src.peekByte(), 0, nil
		}
	}

	bytes, err = src.loadData()

	if err != nil {
		return 0, bytes, err
	} else {
		if remove {
			return src.removeByte(), bytes, nil
		} else {
			return src.peekByte(), bytes, nil
		}
	}
}

func (src *source) removeByte() byte {
	b := src.buff[src.pos]
	src.pos += 1
	return b
}

func (src *source) peekByte() byte {
	return src.buff[src.pos]
}

func (src *source) hasUnreadInput() bool {
	return src.size > 0 && src.pos < src.size
}

func (src *source) loadData() (int, error) {
	nbytes, err := src.fd.Read(src.buff)
	if err != nil {
		return nbytes, err
	}
	src.size = nbytes
	src.pos = 0
	src.reads += 1
	src.in += nbytes
	return nbytes, nil
}

func (src *source) rm() {
	_, _, _ = src.readChar(true)
}

func (src *source) numOfReads() int {
	return src.reads
}

func (src *source) bytesIn() int {
	return src.in
}
