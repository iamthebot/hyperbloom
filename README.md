#HyperBloom
![TravisStatus](https://travis-ci.org/iamthebot/hyperbloom.svg)
[![godoc complete](http://b.repl.ca/v1/godoc-complete-blue.png)](http://godoc.org/github.com/iamthebot/hyperbloom)

![Bloom](https://raw.githubusercontent.com/iamthebot/hyperbloom/master/images/bloom.jpg)

A collection of high performance bloom filter data structures for use in Go. They all use the 64 bit version of Google's XXHASH "extremely fast non-cryptographic" hashing algorithm. Detailed documentation is available via [godoc](http://godoc.org/github.com/iamthebot/hyperbloom).

##BloomFilter
A textbook implementation of a bloom filter. Like StripedBloomFilter, it uses an array of unsigned 64 bit integers. However, it uses centralized locking (via a RWMutex) in place of sharded locking. In addition, it supports non-locking inserts and lookups (InsertAsync) and (LookupAsync). Use if you plan on doing mostly reads and not many writes OR if you plan on using the bloomfilter in a single-threaded scenario (make sure to use InsertAsync and LookupAsync to bypass the mutex in this case)

##StripedBloomFilter
A bloom filter implemented using an array of unsigned 64 bit integers which is divided into 'n' shards. Each shard contains its own mutex, allowing for a high degree of concurrent insert and lookup throughput. One restriction is that the filter size must be a multiple of the number of shards. 

Assuming 16 cores, a 32 shard StriedBloomFilter provides a three order of magnitude decrease in lock contention over a standard bloom filter.

Use if you plan on a roughly equal balance of writes and reads and are using the bloomfilter in a multi-threaded scenario. This variant is especially good for large (1 billion buckets or more) filters.

##NaiveBloomFilter
A bloom filter implemented using a byte array (with each bit in the filter assigned to a byte). This reduces the number of instructions necessary to perform a lookup in the go 1.4 and latest gcc-go compilers. As a result, the NaiveBloomFilter will almost always be faster than the standard variants at the cost of an 8x penalty in memory usage. 

This version uses centralized locking (via a RWMutex) and is perfect for a filter that will mainly be used for reads or in a single threaded context (use the LookupAsync and InsertAsync functions to bypass the mutex in this case). For very small (<20 million buckets) bloom filters, the NaiveBloomFilter can yield enormous performance boosts since most of the filter fits in the processor cache.

##NaiveStripedBloomFilter
A bloom filter implemented using a byte array (with each bit in the filter assigned to a byte) but with distributed locking over 'n' shards. This provides increased concurrent throughput. This is the perfect choice for filters where read performance over multiple threads needs to be maximized (you get a performance gain from not bit mangling).
