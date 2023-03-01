package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

// crc32 | timestamp | keySize | valueSize | key | value
// 4	 | 4		 | 4	   | 4 		   | 4   | 4
func encodeEntry(key, value []byte, keySize, valueSize, ts, entrySize uint32) ([]byte, error) {
	buf := make([]byte, entrySize)
	binary.BigEndian.PutUint32(buf[4:8], ts)
	binary.BigEndian.PutUint32(buf[8:12], keySize)
	binary.BigEndian.PutUint32(buf[12:HeaderSize], valueSize)
	copy(buf[HeaderSize:HeaderSize+keySize], key)
	copy(buf[HeaderSize+keySize:HeaderSize+keySize+valueSize], value)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.BigEndian.PutUint32(buf[0:4], c32)

	return buf, nil
}

/*

keySize	|	valueSize	|	valueOffset	|	key
4   	|   4      		|   8       	|  	xxxxx
*/
func encodeHintData(key []byte, keySize, valueSize, valueOffset uint32) []byte {
	buf := make([]byte, HintHeaderSize+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], keySize)
	binary.LittleEndian.PutUint32(buf[4:8], valueSize)
	binary.LittleEndian.PutUint32(buf[8:HintHeaderSize], valueOffset)
	copy(buf[HintHeaderSize:], []byte(key))

	return buf
}

func decodeHintData(fp *os.File, offset int64) (HintHeader, error) {
	buf, err := read(fp, offset, HintHeaderSize)
	if err != nil {
		return HintHeader{}, err
	}

	keySize := binary.LittleEndian.Uint32(buf[0:4])

	kbuf, err := read(fp, offset+HintHeaderSize, keySize)
	if err != nil {
		return HintHeader{}, err
	}

	valueSize := binary.LittleEndian.Uint32(buf[4:8])
	valueOffset := binary.LittleEndian.Uint32(buf[8:HintHeaderSize])

	return HintHeader{
		ksize:       keySize,
		vsize:       valueSize,
		valueOffset: valueOffset,
		key:         kbuf,
	}, nil
}
