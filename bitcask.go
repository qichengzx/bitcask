package bitcask

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Bitcask struct {
	option   Option
	index    *index
	actFile  *BitFile
	oldFiles *BitFiles
	dir      string
	lock     *os.File
	mu       *sync.RWMutex
}

func New(dir string) (*Bitcask, error) {
	bf, err := newBitFile(dir)
	if err != nil {
		return nil, err
	}
	idx := newIndex()
	lockFile, err := lock(dir)
	if err != nil {
		return nil, err
	}
	option := NewOption(0)
	bitcask := &Bitcask{
		option:   option,
		index:    idx,
		actFile:  bf,
		oldFiles: newBitFiles(),
		dir:      dir,
		lock:     lockFile,
		mu:       &sync.RWMutex{},
	}
	bitcask.loadIndex()
	return bitcask, nil
}

func (b *Bitcask) Close() {
	b.actFile.fp.Close()
	b.lock.Close()
	os.Remove(b.lock.Name())
}

func (b *Bitcask) loadIndex() {
	files, err := readDir(b.dir)
	if err != nil {
		panic(err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ext) {
			continue
		}
		fid, _ := getFid(file.Name())
		var offset int64 = 0
		fp, err := os.Open(filepath.Join(b.dir, file.Name()))
		if err != nil {
			continue
		}

		bitFile, err := toBitFile(fid, fp)
		if err != nil {
			continue
		}
		b.oldFiles.add(fid, bitFile)
		for {
			buf := make([]byte, HeaderSize)
			if _, err := fp.ReadAt(buf, offset); err != nil {
				if err == io.EOF {
					break
				}
			}
			keySize := binary.BigEndian.Uint32(buf[8:12])
			valueSize := binary.BigEndian.Uint32(buf[12:HeaderSize])

			offset += int64(getSize(keySize, valueSize))
			keyByte := make([]byte, keySize)
			if _, err := fp.ReadAt(keyByte, int64(HeaderSize)); err != nil {
				continue
			}
			if valueSize == 0 {
				//key is deleted
				continue
			}
			valByte := make([]byte, valueSize)
			if _, err := fp.ReadAt(valByte, int64(HeaderSize+keySize)); err != nil {

			}

			timestamp := uint64(binary.BigEndian.Uint32(buf[4:8]))

			//load to map
			entry := newEntry(fid, valueSize, uint64(HeaderSize+int64(keySize)), timestamp)
			b.index.put(string(keyByte), entry)
		}
	}
}

func (b *Bitcask) Put(key, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	entry, err := b.actFile.write(key, value)
	if err != nil {
		return err
	}
	b.index.put(string(key), entry)
	return nil
}

func (b *Bitcask) Get(key []byte) ([]byte, error) {
	entry, err := b.index.get(key)
	if err != nil {
		return nil, err
	}

	bf, err := b.checkFileState(entry.fileID)
	if err != nil {
		return nil, err
	}

	value, err := bf.read(entry.valueOffset, entry.valueSize)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (b *Bitcask) Del(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, err := b.index.get(key)
	if err != nil {
		return err
	}

	err = b.actFile.del(key)
	if err != nil {
		return err
	}
	b.index.del(string(key))
	return nil
}

func (b *Bitcask) checkFileState(fid uint32) (*BitFile, error) {
	if fid == b.actFile.fid {
		return b.actFile, nil
	}

	if bf, ok := b.oldFiles.files[fid]; ok {
		return bf, nil
	}

	return nil, errors.New("fid not exist")
}
