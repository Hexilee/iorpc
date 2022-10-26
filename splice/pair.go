package splice

import (
	"fmt"
	"syscall"
)

const (
	SPLICE_F_MOVE     = 0x1
	SPLICE_F_NONBLOCK = 0x2
	SPLICE_F_MORE     = 0x4
	SPLICE_F_GIFT     = 0x8
)

type Pair struct {
	r, w int
	size int
}

func (p *Pair) MaxGrow() {
	for p.Grow(2*p.size) == nil {
	}
}

func (p *Pair) Grow(n int) error {
	if n <= p.size {
		return nil
	}
	if !resizable {
		return fmt.Errorf("splice: want %d bytes, but not resizable", n)
	}
	if n > maxPipeSize {
		return fmt.Errorf("splice: want %d bytes, max pipe size %d", n, maxPipeSize)
	}

	newsize, errNo := fcntl(uintptr(p.r), F_SETPIPE_SZ, n)
	if errNo != 0 {
		return fmt.Errorf("splice: fcntl returned %v", errNo)
	}
	p.size = newsize
	return nil
}

func (p *Pair) Cap() int {
	return p.size
}

func (p *Pair) Close() error {
	err1 := syscall.Close(p.r)
	err2 := syscall.Close(p.w)
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *Pair) Read(d []byte) (n int, err error) {
	return syscall.Read(p.r, d)
}

func (p *Pair) Write(d []byte) (n int, err error) {
	return syscall.Write(p.w, d)
}

func (p *Pair) ReadFd() uintptr {
	return uintptr(p.r)
}

func (p *Pair) WriteFd() uintptr {
	return uintptr(p.w)
}
