package bitcask

import (
	"os"
	"path/filepath"
	"syscall"
)

type FileLock struct {
	fd *os.File
}

const lockFileName = "bitcask.lock"

func AcquireFileLock(path string, readOnly bool) (*FileLock, error) {
	var flag = os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	lockFile := filepath.Join(path, lockFileName)
	file, err := os.OpenFile(lockFile, flag|os.O_EXCL|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	var how = syscall.LOCK_EX | syscall.LOCK_NB
	if readOnly {
		how = syscall.LOCK_SH | syscall.LOCK_NB
	}
	if err := syscall.Flock(int(file.Fd()), how); err != nil {
		return nil, err
	}
	return &FileLock{fd: file}, nil
}

func (fl *FileLock) Release() error {
	flag := syscall.LOCK_UN | syscall.LOCK_NB
	if err := syscall.Flock(int(fl.fd.Fd()), flag); err != nil {
		return err
	}
	fl.fd.Close()
	return os.Remove(fl.fd.Name())
}
