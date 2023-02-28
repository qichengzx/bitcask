package bitcask

import (
	"encoding/binary"
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

/*

keySize	:	valueSize	:	valueOffset	:	key
   4   	:       4      	:       8       :   xxxxx
*/
func encodeHintData(key []byte, keySize, valueSize, valueOffset uint32) []byte {
	buf := make([]byte, HintHeaderSize+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], keySize)
	binary.LittleEndian.PutUint32(buf[4:8], valueSize)
	binary.LittleEndian.PutUint32(buf[8:HintHeaderSize], valueOffset)
	copy(buf[HintHeaderSize:], []byte(key))

	return buf
}

func decodeHintData(fp *os.File, offset int64) (HintHeader, error) {
	buf, err := read(fp, offset, HintHeaderSize)
	if err != nil {
		return HintHeader{}, err
	}

	keySize := binary.LittleEndian.Uint32(buf[0:4])

	kbuf, err := read(fp, offset+HintHeaderSize, keySize)
	if err != nil {
		return HintHeader{}, err
	}

	valueSize := binary.LittleEndian.Uint32(buf[4:8])
	valueOffset := binary.LittleEndian.Uint32(buf[8:HintHeaderSize])

	return HintHeader{
		ksize:       keySize,
		vsize:       valueSize,
		valueOffset: valueOffset,
		key:         kbuf,
	}, nil
}

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
