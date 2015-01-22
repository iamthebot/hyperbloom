package hyperbloom

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

/*
NaiveBloomFilter is a bloomfilter backed by a byte vector rather than a bitvector. As a result, lookups are faster although at an 8x space penalty. It uses central locking via a RWMutex
*/
type NaiveBloomFilter struct {
	bv   []byte        //bytevector
	size uint64        //Size of bytevector. MUST BE A POWER OF 2.
	hf   int           //Number of hash functions
	mut  *sync.RWMutex //Centralized mutex
}

/*
NewNaiveBloomfilter allocates a NaiveBloomFilter with a given size (in bytes) and using a certain number of hashes.
Size must be a power of 2 and larger than 64
*/
func NewNaiveBloomFilter(size uint64, hf int) (*NaiveBloomFilter, error) {
	var bf NaiveBloomFilter
	bf.size = size
	if bf.size < 64 {
		return nil, errors.New("Filter size must be at least 64")
	} else if (bf.size & (bf.size - 1)) != 0 {
		return nil, errors.New("Size must be a power of 2")
	}
	bf.bv = make([]byte, size)
	bf.hf = int(hf)
	bf.mut = &sync.RWMutex{}

	for i := 0; i < len(bf.bv); i++ {
		bf.bv[i] = 0
	}

	return &bf, nil
}

func (bf NaiveBloomFilter) setByte(idx uint64) error {
	if int(idx) >= len(bf.bv) {
		return errors.New("Index can't be larger than filter size")
	}
	bf.mut.Lock()
	bf.bv[idx] = 1
	bf.mut.Unlock()
	return nil
}

func (bf NaiveBloomFilter) setByteAsync(idx uint64) error {
	if int(idx) >= len(bf.bv) {
		return errors.New("Index can't be larger than filter size")
	}
	bf.bv[idx] = 1
	return nil
}

func (bf NaiveBloomFilter) getByte(idx uint64) (bool, error) {
	if idx >= bf.size {
		return false, errors.New("Index can't be larger than filter size")
	}
	bf.mut.RLock()
	switch bf.bv[idx] {
	case 1:
		bf.mut.RUnlock()
		return true, nil
	default:
		bf.mut.RUnlock()
		return false, nil
	}
}

func (bf NaiveBloomFilter) getByteAsync(idx uint64) (bool, error) {
	if idx >= bf.size {
		return false, errors.New("Index can't be larger than filter size")
	}

	switch bf.bv[idx] {
	case 1:
		return true, nil
	default:
		return false, nil
	}
}

/*Looks up an entry into the NaiveBloomFilter. Returns true if a match is found, false otherwise. If an error occurs, will also return false.
This perform a reader lock on the filter (writers must wait until all active readers finish).
*/
func (bf NaiveBloomFilter) Lookup(entry string) (bool, error) {
	hashes := hashEntry([]byte(entry), bf.hf)
	for i := 0; i < bf.hf; i++ {
		lookup_idx := hashes[i] & (bf.size - 1)
		if exists, err := bf.getByte(lookup_idx); !exists {
			if err != nil {
				return false, err
			}
			return false, nil
		}
	}
	return true, nil
}

/*Looks up an entry into the NaiveBloomFilter. Returns true if a match is found, false otherwise. If an error occurs, will also return false.
This won't lock the filter.
*/
func (bf NaiveBloomFilter) LookupAsync(entry string) (bool, error) {
	hashes := hashEntry([]byte(entry), bf.hf)
	for i := 0; i < bf.hf; i++ {
		lookup_idx := hashes[i] & (bf.size - 1)
		if exists, err := bf.getByteAsync(lookup_idx); !exists {
			if err != nil {
				return false, err
			}
			return false, nil
		}
	}
	return true, nil
}

/*Inserts an entry into the NaiveBloomFilter. Locks the filter.*/
func (bf NaiveBloomFilter) Insert(entry string) error {
	hashes := hashEntry([]byte(entry), bf.hf)
	for i := 0; i < bf.hf; i++ {
		insert_idx := hashes[i] & (bf.size - 1)
		err := bf.setByte(insert_idx)
		if err != nil {
			return err
		}
	}
	return nil
}

/*Inserts an entry into the NaiveBloomFilter. Does not lock the filter.*/
func (bf NaiveBloomFilter) InsertAsync(entry string) error {
	hashes := hashEntry([]byte(entry), bf.hf)
	for i := 0; i < bf.hf; i++ {
		insert_idx := hashes[i] & (bf.size - 1)
		err := bf.setByteAsync(insert_idx)
		if err != nil {
			return err
		}
	}
	return nil
}

/*Writes underlying byte vector to a file.*/
func (bf NaiveBloomFilter) Write(filename string) error {
	var err error
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	defer f.Close()
	if err != nil {
		return err
	}
	var b bytes.Buffer
	for i := 0; i < int(bf.size); i++ {
		bin := fmt.Sprintf("%d", bf.bv[i])
		b.Write([]byte(bin))
	}
	bv := b.Bytes()
	log.Println("Writing byte vector to file...")
	f.Write(bv)
	f.Close()
	log.Printf("Successfully wrote bytevector to file: %s\n", filename)
	return nil
}

/*Merges current byte vector with one loaded from a file.
This locks the filter for each nonzero byte being written.
*/
func (bf NaiveBloomFilter) Load(filename string) error {
	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		return err
	}

	buf_rdr := bufio.NewReader(f)
	bv, err := buf_rdr.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return err
	}
	log.Println("Bytes read successfully")
	if bf.size != uint64(len(bv)-1) {
		return errors.New(fmt.Sprintf("File bitvector size: %d. Specified size: %d. Mismatch.", len(bv)-1, bf.size))
	}
	for i := 0; i < len(bv); i++ {
		if bv[i] > 0 {
			bf.setByte(uint64(i))
		}
	}
	fmt.Printf("Loaded bitvector from file: %s\n", filename)
	return nil
}
