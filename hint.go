package bitcask

import (
	"fmt"
	"os"
)

func putHint(fp *os.File, key []byte, keySize, valueSize, valueOffset uint32) (int, error) {
	buf := encodeHintData(key, keySize, valueSize, valueOffset)

	return fp.Write(buf)
}

type HintHeader struct {
	ksize       uint32
	vsize       uint32
	valueOffset uint32
	timestamp   uint64
	key         []byte
}

const (
	HintHeaderSize = 16
)

func newHintFile(path string, fid uint32) (*os.File, error) {
	fname := getHintFileName(path, fid)
	return os.OpenFile(fname, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
}

func openHintFile(path string, fid uint32) (*os.File, error) {
	fname := getHintFileName(path, fid)
	return os.OpenFile(fname, os.O_RDONLY, 0)
}

const hintFileExt = ".hint"

func getHintFileName(path string, fid uint32) string {
	return fmt.Sprintf("%s%s%d%s", path, string(os.PathSeparator), fid, hintFileExt)
}
