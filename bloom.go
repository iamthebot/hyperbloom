/*
A high performance concurrent bloom filter library with striped and naive implementations
*/
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
BloomFilter is a bloomfilter backed by an array of unsigned 64 bit integers (with bits encoded in each one). It uses central locking via a RWMutex and supports both synchronous and asynchronous inserts and lookups
*/
type BloomFilter struct {
	bv   []uint64      //bitvector
	size uint64        //Size of bitvector. MUST BE A POWER OF 2.
	hf   int           //Number of hash functions
	mut  *sync.RWMutex //Centralized mutex
}

/*NewBloomfilter allocates a BloomFilter with a given size (in bits) and using a certain number of hashes.
Size must be a power of 2 and larger than 64
*/
func NewBloomFilter(size uint64, hf int) (*BloomFilter, error) {
	var bf BloomFilter
	bf.size = size
	if bf.size < 64 {
		return nil, errors.New("Filter size must be at least 64")
	} else if (bf.size & (bf.size - 1)) != 0 {
		return nil, errors.New("Size must be a power of 2")
	}
	bf.bv = make([]uint64, size/64)
	bf.hf = int(hf)
	bf.mut = &sync.RWMutex{}

	for i := 0; i < len(bf.bv); i++ {
		bf.bv[i] = 0
	}

	return &bf, nil
}

func (bf BloomFilter) setBit(idx uint64) error {
	if idx > (bf.size - 1) {
		return errors.New("Index can't be larger than filter size")
	}
	intID := idx / 64
	bitID := idx & 63
	bf.mut.Lock()
	bf.bv[intID] |= (1 << bitID)
	bf.mut.Unlock()
	return nil
}

func (bf BloomFilter) setBitAsync(idx uint64) error {
	if idx > (bf.size - 1) {
		return errors.New("Index can't be larger than filter size")
	}
	intID := idx / 64
	bitID := idx & 63
	bf.bv[intID] |= (1 << bitID)
	return nil
}

func (bf BloomFilter) getBit(idx uint64) (bool, error) {
	if idx > (bf.size - 1) {
		return false, errors.New("Index can't be larger than filter size")
	}

	intID := idx / 64
	bitID := idx & 63
	bf.mut.RLock()
	exists := !(bf.bv[intID]&(1<<bitID) == 0)
	bf.mut.RUnlock()
	return exists, nil
}

func (bf BloomFilter) getBitAsync(idx uint64) (bool, error) {
	if idx > (bf.size - 1) {
		return false, errors.New("Index can't be larger than filter size")
	}
	intID := idx / 64
	bitID := idx & 63
	exists := !(bf.bv[intID]&(1<<bitID) == 0)
	return exists, nil
}

/*Looks up an entry into the BloomFilter. Returns true if a match is found, false otherwise. If an error occurs, will also return false.
This perform a reader lock on the filter (writers must wait until all active readers finish).
*/
func (bf BloomFilter) Lookup(entry string) (bool, error) {
	hashes := hashEntry([]byte(entry), bf.hf)
	for i := 0; i < bf.hf; i++ {
		lookup_idx := hashes[i] & (bf.size - 1)
		if exists, err := bf.getBit(lookup_idx); !exists {
			if err != nil {
				return false, err
			}
			return false, nil
		}
	}
	return true, nil
}

/*Looks up an entry into the BloomFilter. Returns true if a match is found, false otherwise. If an error occurs, will also return false.
This won't lock the filter.
*/
func (bf BloomFilter) LookupAsync(entry string) (bool, error) {
	hashes := hashEntry([]byte(entry), bf.hf)
	for i := 0; i < bf.hf; i++ {
		lookup_idx := hashes[i] & (bf.size - 1)
		if exists, err := bf.getBitAsync(lookup_idx); !exists {
			if err != nil {
				return false, err
			}
			return false, nil
		}
	}
	return true, nil
}

/*Inserts an entry into the NaiveBloomFilter. Locks the filter.*/
func (bf BloomFilter) Insert(entry string) error {
	hashes := hashEntry([]byte(entry), bf.hf)
	for i := 0; i < bf.hf; i++ {
		insert_idx := hashes[i] & (bf.size - 1)
		err := bf.setBit(insert_idx)
		if err != nil {
			return err
		}
	}
	return nil
}

/*Inserts an entry into the NaiveBloomFilter. Doesn't lock the filter.*/
func (bf BloomFilter) InsertAsync(entry string) error {
	hashes := hashEntry([]byte(entry), bf.hf)
	for i := 0; i < bf.hf; i++ {
		insert_idx := hashes[i] & (bf.size - 1)
		err := bf.setBitAsync(insert_idx)
		if err != nil {
			return err
		}
	}
	return nil
}

/*Writes underlying bit vector to a file.*/
func (bf BloomFilter) Write(filename string) error {
	var err error
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
	defer f.Close()
	if err != nil {
		return err
	}
	var b bytes.Buffer
	for i := 0; i < int(bf.size); i++ {
		bin := fmt.Sprintf("%b", bf.bv[i])
		b.Write([]byte(bin))
	}
	bv := b.Bytes()
	log.Println("Writing bit vector to file...")
	f.Write(bv)
	f.Close()
	log.Printf("Successfully wrote bitvector to file: %s\n", filename)
	return nil
}

/*Merges current bit vector with one loaded from a file.
This locks the filter for each nonzero byte being written.
*/
func (bf BloomFilter) Load(filename string) error {
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
			bf.setBit(uint64(i))
		}
	}
	fmt.Printf("Loaded bitvector from file: %s\n", filename)
	return nil
}
