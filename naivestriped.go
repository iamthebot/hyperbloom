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
NaiveStripedBloomFilter is a bloomfilter backed by a byte vector rather than a bit vector. It uses distributed locking via striping and supports both synchronous and asynchronous inserts and lookups.
*/
type NaiveStripedBloomFilter struct {
	bv       []byte        //bitvector
	size     uint64        //Size of bitvector. MUST BE A POWER OF 2.
	shards   uint64        //Number of shards. size must be multiple of shards.
	hf       int           //Number of hash functions
	mutArr   []*sync.Mutex //Mutex for each shard
	shardLen uint64        //Precomputed number of bits per shard
}

/*NewNaiveStripedBloomfilter allocates a NaiveStripedBloomFilter with a given size (in bits) and using a certain number of hashes.
Size must be a power of 2 and larger than 64.
Shards must be a power of 2 (smaller than size) and cannot exceed size/64.
*/
func NewNaiveStripedBloomFilter(size uint64, hf int, shards uint64) (*NaiveStripedBloomFilter, error) {
	/*
	 * Create a new bloomfilter of size "size"
	 * The bloomfilter will hash "hf" times
	 */
	var bf NaiveStripedBloomFilter
	bf.size = size
	bf.shards = shards
	if bf.size < 64 {
		return nil, errors.New("Filter size must be at least 64")
	} else if (bf.size & (bf.size - 1)) != 0 {
		return nil, errors.New("Size must be a power of 2")
	} else if (bf.shards & (bf.shards - 1)) != 0 {
		return nil, errors.New("Shards must be a power of 2")
	} else if bf.shards > bf.size/64 {
		return nil, errors.New("Shards cannot exceed size/64")
	} else if bf.size%bf.shards != 0 {
		return nil, errors.New("Size must be a multiple of shards")
	}
	bf.bv = make([]byte, size)
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

func (bf NaiveStripedBloomFilter) setByte(idx uint64) error {
	if idx > (bf.size - 1) {
		return errors.New("Index can't be larger than filter size")
	}
	shardID := idx / bf.shardLen
	bf.mutArr[shardID].Lock()
	bf.bv[idx] = 1
	bf.mutArr[shardID].Unlock()
	return nil
}

func (bf NaiveStripedBloomFilter) setByteAsync(idx uint64) error {
	if idx > (bf.size - 1) {
		return errors.New("Index can't be larger than filter size")
	}
	bf.bv[idx] = 1
	return nil
}

func (bf NaiveStripedBloomFilter) getByte(idx uint64) (bool, error) {
	if idx > (bf.size - 1) {
		return false, errors.New("Index can't be larger than filter size")
	}

	shardID := idx / bf.shardLen
	bf.mutArr[shardID].Lock()
	exists := (bf.bv[idx] == 1)
	bf.mutArr[shardID].Unlock()
	return exists, nil
}

func (bf NaiveStripedBloomFilter) getByteAsync(idx uint64) (bool, error) {
	if idx > (bf.size - 1) {
		return false, errors.New("Index can't be larger than filter size")
	}
	exists := (bf.bv[idx] == 1)
	return exists, nil
}

/*Looks up an entry in the NaiveStripedBloomFilter. Returns true if a match is found, false otherwise. If an error occurs, will also return false. This will lock one shard of the filter.
 */
func (bf NaiveStripedBloomFilter) Lookup(entry string) (bool, error) {
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

/*Looks up an entry in the NaiveStripedBloomFilter. Returns true if a match is found, false otherwise. If an error occurs, will also return false.
This won't lock the filter.
*/
func (bf NaiveStripedBloomFilter) LookupAsync(entry string) (bool, error) {
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

/*Inserts an entry into the NaiveStripedBloomFilter. Locks one shard of the filter.*/
func (bf NaiveStripedBloomFilter) Insert(entry string) error {
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

/*Inserts an entry into the NaiveBloomFilter. Doesn't lock the filter.*/
func (bf NaiveStripedBloomFilter) InsertAsync(entry string) error {
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

/*Writes underlying bit vector to a file.*/
func (bf NaiveStripedBloomFilter) Write(filename string) error {
	/*Writes bit vector to file*/
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
	log.Println("Writing bit vector to file...")
	f.Write(bv)
	f.Close()
	log.Printf("Successfully wrote bitvector to file: %s\n", filename)
	return nil
}

/*Merges current bit vector with one loaded from a file.
This locks the filter for each nonzero byte being written.
*/
func (bf NaiveStripedBloomFilter) Load(filename string) error {
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
