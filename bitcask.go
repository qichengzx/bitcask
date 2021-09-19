package bitcask

import (
	"sync"
)

type Bitcask struct {
	index   *index
	actFile *BitFile
	mu      *sync.RWMutex
}

func New(dir string) (*Bitcask, error) {
	bf, err := newBitFile(dir)
	if err != nil {
		return nil, err
	}
	idx := newIndex()
	return &Bitcask{
		index:   idx,
		actFile: bf,
		mu:      &sync.RWMutex{},
	}, nil
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
