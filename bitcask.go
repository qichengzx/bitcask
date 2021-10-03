package bitcask

import (
	"os"
	"sync"
)

type Bitcask struct {
	index   *index
	actFile *BitFile
	dir     string
	lock    *os.File
	mu      *sync.RWMutex
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
	return &Bitcask{
		index:   idx,
		actFile: bf,
		dir:     dir,
		lock:    lockFile,
		mu:      &sync.RWMutex{},
	}, nil
}

func (b *Bitcask) Close() {
	b.actFile.fp.Close()
	b.lock.Close()
	os.Remove(b.lock.Name())
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

	value, err := b.actFile.read(entry.valueOffset, entry.valueSize)
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
