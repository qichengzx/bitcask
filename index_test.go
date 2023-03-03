package bitcask

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	key = "keyname"
	idx = newIndex()
	ety = newEntry(1, uint32(len(key)), 100, 20, uint64(time.Now().Unix()))
)

func TestIndexPut(t *testing.T) {
	idx.put(key, ety)
}

func TestIndexGet(t *testing.T) {
	e, err := idx.get([]byte(key))
	if err != nil {
		t.Fatalf("key %s not found", key)
	}
	assert.Equal(t, e, ety)
}

func TestIndexNotFound(t *testing.T) {
	_, err := idx.get([]byte("notexists"))
	assert.Equal(t, err, ErrKeyNotFound)
}
