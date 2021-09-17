package bitcask

import "sync"

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
