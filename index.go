package bitcask

import (
	"errors"
	"sync"
)

type index struct {
	entrys map[string]*entry
	*sync.RWMutex
}

func newIndex() *index {
	return &index{
		entrys:  make(map[string]*entry),
		RWMutex: &sync.RWMutex{},
	}
}

func (i *index) put(key string, entry *entry) {
	i.Lock()
	defer i.Unlock()
	i.entrys[key] = entry
}

func (i *index) get(key []byte) (*entry, error) {
	i.Lock()
	defer i.Unlock()
	if entry, ok := i.entrys[string(key)]; ok {
		return entry, nil
	}

	return nil, errors.New("key not found")
}