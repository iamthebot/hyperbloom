package hyperbloom

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSBFSetBit(t *testing.T) {
	bf, err := NewStripedBloomFilter(1048576, 4, 64)
	assert.Nil(t, err)
	err = bf.setBit(100)
	assert.Nil(t, err)

	bitExists, err := bf.getBit(100)
	assert.Nil(t, err)
	assert.Equal(t, true, bitExists)

	falseBit, err := bf.getBit(1048575)
	assert.Nil(t, err)
	assert.Equal(t, false, falseBit)
}

func TestNewStripedBloomFilter(t *testing.T) {
	bf, err := NewStripedBloomFilter(100000, 4, 10)
	assert.NotNil(t, err)
	assert.Nil(t, bf)

	bf, err = NewStripedBloomFilter(1048576, 4, 10)
	assert.NotNil(t, err)
	assert.Nil(t, bf)
}

func TestStripedBloomFilter(t *testing.T) {
	bf, err := NewStripedBloomFilter(1048576, 4, 64)
	assert.Nil(t, err)
	e1 := "b99afb65c9f97b2e0feea844eea55f69"
	e2 := "f530e3093a1617d64f400c5578005b7c"
	e3 := "b29317ac342ceafc79e59996678efeb3"
	e4 := "00421829519ccc2834eedc2bac21df68"
	fake1 := "hahaidontexist"
	fake2 := "foobar"
	fake3 := "turnips"
	fake4 := "lavacakes"

	bf.Insert(e1)
	bf.Insert(e2)
	bf.Insert(e3)
	bf.Insert(e4)

	e1Exists, err := bf.Lookup(e1)
	assert.Nil(t, err)
	assert.Equal(t, true, e1Exists)

	e2Exists, err := bf.Lookup(e2)
	assert.Nil(t, err)
	assert.Equal(t, true, e2Exists)

	e3Exists, err := bf.Lookup(e3)
	assert.Nil(t, err)
	assert.Equal(t, true, e3Exists)

	e4Exists, err := bf.Lookup(e4)
	assert.Nil(t, err)
	assert.Equal(t, true, e4Exists)

	fake1Exists, err := bf.Lookup(fake1)
	assert.Nil(t, err)
	assert.Equal(t, false, fake1Exists)

	fake2Exists, err := bf.Lookup(fake2)
	assert.Nil(t, err)
	assert.Equal(t, false, fake2Exists)

	fake3Exists, err := bf.Lookup(fake3)
	assert.Nil(t, err)
	assert.Equal(t, false, fake3Exists)

	fake4Exists, err := bf.Lookup(fake4)
	assert.Nil(t, err)
	assert.Equal(t, false, fake4Exists)
}
