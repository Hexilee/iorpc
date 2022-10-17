package iorpc

import (
	"errors"
	"os"
	"syscall"
)

func Pipe(size uintptr) (reader *os.File, writer *os.File, err error) {
	reader, writer, err = os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, writer.Fd(), syscall.F_SETPIPE_SZ, size)
	if errno != 0 {
		return nil, nil, errno
	}
	return reader, writer, errors.New("noop")
}
