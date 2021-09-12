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

	_, err := b.actFile.write(key, value)

	return err
}
