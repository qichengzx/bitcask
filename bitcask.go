package bitcask

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Bitcask struct {
	option   Option
	index    *index
	lock     *FileLock
	oldFiles *BitFiles
	actFile  *BitFile
	mu       *sync.RWMutex
}

func New(dir string) (*Bitcask, error) {
	bf, err := newBitFile(dir)
	if err != nil {
		return nil, err
	}
	fileLock, err := AcquireFileLock(dir, false)
	if err != nil {
		return nil, err
	}

	options := NewOption(dir, 0)

	bitcask := &Bitcask{
		option:   options,
		index:    newIndex(),
		lock:     fileLock,
		oldFiles: newBitFiles(),
		actFile:  bf,
		mu:       &sync.RWMutex{},
	}
	bitcask.loadIndex()
	bitcask.merge()
	return bitcask, nil
}

func (b *Bitcask) Close() {
	b.actFile.fp.Close()
	b.lock.Release()
}

func (b *Bitcask) loadIndex() {
	log.Println("Start load old files.")
	t1 := time.Now()

	files, err := scanOldFiles(b.option.Dir)
	if err != nil {
		panic(err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for _, file := range files {
		if filepath.Base(b.actFile.fp.Name()) == file.Name() { //skip active file
			continue
		}
		fid, _ := getFid(file.Name())
		fmt.Println(file.Name(), fid)
		var offset int64 = 0

		hintFp, err := openHintFile(b.option.Dir, fid)
		if err == nil && hintFp != nil {
			b.index.buildFromHint(fid, hintFp)
		} else {
			fp, _ := os.Open(filepath.Join(b.option.Dir, file.Name()))

			bitFile, err := toBitFile(fid, fp)
			if err != nil {
				continue
			}

			hintfp, _ := newHintFile(b.option.Dir, fid)
			b.oldFiles.add(fid, bitFile)
			for {
				entry, entrySize := bitFile.newEntryFromBuf(offset)
				if entry == nil {
					break
				}
				offset += int64(entrySize)

				if entry.valueSize == 0 {
					b.index.del(string(entry.key))
					//key was deleted
					continue
				}

				//load to map
				b.index.put(string(entry.key), entry)
				putHint(hintfp, entry.key, uint32(len(entry.key)), uint32(entry.valueSize), uint32(offset))
			}
		}
	}
	log.Println("load old file use:", time.Since(t1).String())
}
func (b *Bitcask) Put(key, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkFile()

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

	bf, err := b.checkFileState(entry.fileID)
	if err != nil {
		return nil, err
	}

	value, err := bf.read(entry.valueOffset, entry.valueSize)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (b *Bitcask) Del(key []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	_, err := b.index.get(key)
	if err != nil {
		return nil
	}

	b.checkFile()

	err = b.actFile.del(key)
	if err != nil {
		return err
	}
	b.index.del(string(key))
	return nil
}

func (b *Bitcask) checkFileState(fid uint32) (*BitFile, error) {
	if fid == b.actFile.fid {
		return b.actFile, nil
	}

	if bf, ok := b.oldFiles.files[fid]; ok {
		return bf, nil
	}

	return nil, errors.New("fid not exist")
}

func (b *Bitcask) checkFile() error {
	if b.actFile.offset > b.option.MaxFileSize {
		b.actFile.fp.Close()
		b.oldFiles.add(b.actFile.fid, b.actFile)

		bf, err := newBitFile(b.option.Dir)
		if err != nil {
			return err
		}

		b.actFile = bf
	}

	return nil
}
