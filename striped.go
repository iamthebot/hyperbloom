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
StripedBloomFilter is a bloomfilter backed by an array of unsigned 64 bit integers (with bits encoded in each one). It uses distributed locking via striping and supports both synchronous and asynchronous inserts and lookups.
*/
type StripedBloomFilter struct {
	bv       []uint64      //bitvector
	size     uint64        //Size of bitvector. MUST BE A POWER OF 2.
	shards   uint64        //Number of shards. size must be multiple of shards.
	hf       int           //Number of hash functions
	mutArr   []*sync.Mutex //Mutex for each shard
	shardLen uint64        //Precomputed number of bits per shard
}

/*NewBloomfilter allocates a StripedBloomFilter with a given size (in bits) and using a certain number of hashes.
Size must be a power of 2 and larger than 64.
Shards must be a power of 2 (smaller than size) and cannot exceed size/64.
*/
func NewStripedBloomFilter(size uint64, hf int, shards uint64) (*StripedBloomFilter, error) {
	var bf StripedBloomFilter
	bf.size = size
	bf.shards = shards
	if bf.size < 64 {
		return nil, errors.New("Filter size must be at least 64")
	} else if bf.shards == 0 {
		return nil, errors.New("Shards must be nonzero")
	} else if (bf.size & (bf.size - 1)) != 0 {
		return nil, errors.New("Size must be a power of 2")
	} else if (bf.shards & (bf.shards - 1)) != 0 {
		return nil, errors.New("Shards must be a power of 2")
	} else if bf.shards > bf.size/64 {
		return nil, errors.New("Shards cannot exceed size/64")
	} else if bf.size%bf.shards != 0 {
		return nil, errors.New("Size must be a multiple of shards")
	}
	bf.bv = make([]uint64, size/64)
	bf.hf = int(hf)
	bf.mutArr = make([]*sync.Mutex, shards)

	for i := 0; i < int(shards); i++ {
		bf.mutArr[i] = &sync.Mutex{}
	}
	for i := 0; i < len(bf.bv); i++ {
		bf.bv[i] = 0
	}

	bf.shardLen = bf.size / bf.shards

	return &bf, nil
}

func (bf StripedBloomFilter) setBit(idx uint64) error {
	if idx > (bf.size - 1) {
		return errors.New("Index can't be larger than filter size")
	}
	shardID := idx / bf.shardLen
	intID := idx / 64
	bitID := idx & 63 //x Mod y = x & (y-1) if y is power of 2
	bf.mutArr[shardID].Lock()
	bf.bv[intID] |= (1 << bitID)
	bf.mutArr[shardID].Unlock()
	return nil
}

func (bf StripedBloomFilter) setBitAsync(idx uint64) error {
	if idx > (bf.size - 1) {
		return errors.New("Index can't be larger than filter size")
	}
	intID := idx / 64
	bitID := idx & 63 //x Mod y = x & (y-1) if y is power of 2
	bf.bv[intID] |= (1 << bitID)
	return nil
}

func (bf StripedBloomFilter) getBit(idx uint64) (bool, error) {
	if idx > (bf.size - 1) {
		return false, errors.New("Index can't be larger than filter size")
	}

	shardID := idx / bf.shardLen
	intID := idx / 64 //which int64 in the bitvector do we want?
	bitID := idx & 63 //which bit in that int64 do we want?
	bf.mutArr[shardID].Lock()
	exists := !(bf.bv[intID]&(1<<bitID) == 0)
	bf.mutArr[shardID].Unlock()
	return exists, nil
}

func (bf StripedBloomFilter) getBitAsync(idx uint64) (bool, error) {
	if idx > (bf.size - 1) {
		return false, errors.New("Index can't be larger than filter size")
	}

	intID := idx / 64 //which int64 in the bitvector do we want?
	bitID := idx & 63 //which bit in that int64 do we want?
	exists := !(bf.bv[intID]&(1<<bitID) == 0)
	return exists, nil
}

/*Looks up an entry in the StripedBloomFilter. Returns true if a match is found, false otherwise. If an error occurs, will also return false.
This perform a reader lock on the filter (writers must wait until all active readers finish).
*/
func (bf StripedBloomFilter) Lookup(entry string) (bool, error) {
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

/*Looks up an entry in the StripedBloomFilter. Returns true if a match is found, false otherwise. If an error occurs, will also return false.
This won't lock the filter.
*/
func (bf StripedBloomFilter) LookupAsync(entry string) (bool, error) {
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

/*Inserts an entry into the StripedBloomFilter. Locks the filter.*/
func (bf StripedBloomFilter) Insert(entry string) error {
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

/*Inserts an entry into the StripedBloomFilter. Doesn't lock the filter.*/
func (bf StripedBloomFilter) InsertAsync(entry string) error {
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
func (bf StripedBloomFilter) Write(filename string) error {
	/*Writes bit vector to file*/
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
func (bf StripedBloomFilter) Load(filename string) error {
	/*Loads bit vector from file*/
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
	bf.size = uint64(len(bv) - 1)
	bf.bv = make([]uint64, bf.size-1)
	for i := 0; i < len(bv); i++ {
		if bv[i] > 0 {
			bf.setBit(uint64(i))
		}
	}
	fmt.Printf("Loaded bitvector from file: %s\n", filename)
	return nil
}
