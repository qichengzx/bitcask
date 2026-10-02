package bitcask

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildFromDataHintValueOffset(t *testing.T) {
	dir := t.TempDir()

	bf, err := newBitFile(dir)
	assert.Nil(t, err)
	defer bf.fp.Close()

	writes := map[string]string{
		"alpha": "one",
		"beta":  "two",
		"gamma": "three",
	}
	for k, v := range writes {
		_, err := bf.write([]byte(k), []byte(v))
		assert.Nil(t, err)
	}

	b := &Bitcask{index: newIndex()}
	hintFp, err := newHintFile(dir, bf.fid)
	assert.Nil(t, err)
	b.buildFromData(bf, hintFp)
	hintFp.Close()

	hintFp, err = openHintFile(dir, bf.fid)
	assert.Nil(t, err)
	defer hintFp.Close()

	rebuilt := newIndex()
	rebuilt.buildFromHint(bf.fid, hintFp)

	for k, v := range writes {
		entry, err := rebuilt.get([]byte(k))
		assert.Nil(t, err)

		got, err := bf.read(entry.valueOffset, entry.valueSize)
		assert.Nil(t, err)
		assert.Equal(t, v, string(got))
	}
}
