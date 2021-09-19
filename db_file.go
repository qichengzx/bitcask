package bitcask

import (
	"fmt"
	"os"
	"time"
)

type BitFile struct {
	fp     *os.File
	fid    uint32
	offset uint64
}

func newBitFile(dir string) (*BitFile, error) {
	bf := &BitFile{}
	fp, err := bf.openFile(dir)
	if err != nil {
		return nil, err
	}
	bf.fp = fp

	return bf, nil
}

func (bf *BitFile) write(key, value []byte) (*entry, error) {
	ts := uint32(time.Now().Unix())

	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	entrySize := getSize(keySize, valueSize)
	buf, _ := encode(key, value, keySize, valueSize, ts, entrySize)

	offset := bf.offset + uint64(HeaderSize+keySize)

	_, err := bf.fp.WriteAt(buf, int64(bf.offset))
	if err != nil {
		panic(err)
	}

	bf.offset += uint64(entrySize)

	return &entry{
		fileID:      bf.fid,
		valueSize:   valueSize,
		valueOffset: offset,
		timestamp:   uint64(ts),
	}, nil
}

func (bf *BitFile) openFile(dir string) (*os.File, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	file := bf.newFile(dir)
	fp, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return fp, nil
}

func (bf *BitFile) read(offset uint64, length uint32) ([]byte, error) {
	value := make([]byte, length)
	bf.fp.Seek(int64(offset), 0)
	_, err := bf.fp.Read(value)
	if err != nil {
		return nil, err
	}
	return value, err
}


func (bf *BitFile) newFile(dir string) string {
	bf.fid++
	return newFilePath(dir, bf.fid)
}

func newFilePath(dir string, fid uint32) string {
	return fmt.Sprintf("%s%s%06d.%s", dir, string(os.PathSeparator), fid, "data")
}
