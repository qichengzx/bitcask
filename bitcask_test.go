package bitcask

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	dir     = "/tmp/"
	testkey = []byte("key")
	testval = []byte("value")
)

func TestNewBitcask(t *testing.T) {
	bc, err := New(dir)

	assert.Nil(t, err)
	assert.NotNil(t, bc)

	defer bc.Close()

	err = bc.Put(testkey, testval)
	assert.Nil(t, err)

	v, err := bc.Get(testkey)
	assert.Nil(t, err)
	assert.Equal(t, v, testval)
}
