package bitcask

type entry struct {
	fileID      uint32
	keySize     uint32
	valueSize   uint32
	valueOffset uint64
	timestamp   uint64
	key         []byte
}

const HeaderSize = 16

func newEntry(fid, keySize, valueSize uint32, valueOffset, timestamp uint64) *entry {
	return &entry{
		fileID:      fid,
		keySize:     keySize,
		valueSize:   valueSize,
		valueOffset: valueOffset,
		timestamp:   timestamp,
	}
}

func getSize(keySize, valueSize uint32) uint32 {
	return HeaderSize + keySize + valueSize
}
