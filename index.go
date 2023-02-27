package bitcask

import (
	"errors"
	"io"
	"os"
	"sync"
)

type index struct {
	entrys map[string]*entry
	mu     *sync.RWMutex
}

var (
	ErrKeyNotFound = errors.New("key not found")
)

func newIndex() *index {
	return &index{
		entrys: make(map[string]*entry),
		mu:     &sync.RWMutex{},
	}
}

func (i *index) put(key string, entry *entry) {
	i.mu.Lock()
	i.entrys[key] = entry
	i.mu.Unlock()
}

func (i *index) get(key []byte) (*entry, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if entry, ok := i.entrys[string(key)]; ok {
		return entry, nil
	}

	return nil, ErrKeyNotFound
}

func (i *index) del(key string) {
	i.mu.Lock()
	delete(i.entrys, key)
	i.mu.Unlock()
}

func (i *index) buildFromHint(fid uint32, hintFp *os.File) {
	var offset int64 = 0
	for {
		header, err := decodeHintData(hintFp, offset)
		if err != nil && err == io.EOF {
			//TODO
			break
		}

		entry := newEntry(fid, header.ksize, header.vsize, uint64(header.valueOffset), header.timestamp)
		i.put(string(header.key), entry)

		offset += int64(header.ksize) + int64(HintHeaderSize)
	}
}
