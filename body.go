package iorpc

import (
	"io"
	"os"
	"syscall"

	"github.com/hexilee/iorpc/splice"
	"github.com/pkg/errors"
)

type Body struct {
	Offset, Size uint64
	Reader       io.ReadCloser
	NotClose     bool
}

type Pipe struct {
	pair *splice.Pair
}

type IsFile interface {
	io.Closer
	File() *os.File
}

type IsConn interface {
	io.Closer
	syscall.Conn
}

type IsPipe interface {
	io.ReadCloser
	ReadFd() (fd uintptr)
	WriteTo(fd uintptr, n int, flags int) (int, error)
}

type IsBuffer interface {
	io.Closer
	Iovec() [][]byte
}

func (b *Body) Reset() {
	b.Offset, b.Size, b.Reader, b.NotClose = 0, 0, nil, false
}

func (b *Body) Close() error {
	if b.NotClose || b.Reader == nil {
		return nil
	}
	return b.Reader.Close()
}

func alignSize(size int) int {
	pageSize := os.Getpagesize()
	return size + pageSize - size%pageSize
}

func PipeConn(r IsConn, size int) (IsPipe, error) {
	pair, err := splice.Get()
	if err != nil {
		return nil, errors.Wrap(err, "get pipe pair")
	}
	err = pair.Grow(alignSize(size))
	if err != nil {
		return nil, errors.Wrap(err, "grow pipe pair")
	}

	if _, err = pair.LoadConn(r, size); err != nil {
		return nil, errors.Wrap(err, "pair load conn")
	}
	return &Pipe{pair: pair}, nil
}

func (p *Pipe) ReadFd() uintptr {
	return p.pair.ReadFd()
}

func (p *Pipe) WriteTo(fd uintptr, n int, flags int) (int, error) {
	if p.pair == nil {
		return 0, io.EOF
	}
	return p.pair.WriteTo(fd, n, flags)
}

func (p *Pipe) Read(b []byte) (int, error) {
	if p.pair == nil {
		return 0, io.EOF
	}
	return p.pair.Read(b)
}

func (p *Pipe) Close() error {
	if p.pair == nil {
		return nil
	}
	splice.Done(p.pair)
	return nil
}
