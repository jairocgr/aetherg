package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHashMapRW(t *testing.T) {
	assert := assert.New(t)

	key := "F398BC5672A51D8D"
	val := []byte("71A79DF49BDC291E1578986A71929")
	exp := 360

	hm := newHashmap()
	hm.set(key, val, exp)

	item, found := hm.get(key)

	assert.Equal(found, true)
	assert.Equal(item.getKey(), key)
	assert.Equal(item.getValue(), val)
	assert.Equal(item.toString(), string(val))
	assert.Equal(item.getExpiration(), exp)

	hm.rm(key)
	_, found = hm.get(key)
	assert.Equal(found, false)
}

func TestExpirationOfKeys(t *testing.T) {
	assert := assert.New(t)

	key := "F398BC5672A51D8D"
	val := []byte("71A79DF49BDC291E1578986A71929")
	exp := 2

	hm := newHashmap()
	hm.set(key, val, exp)

	time.Sleep(3 * time.Second)
	hm.evict()

	_, found := hm.get(key)
	assert.Equal(found, false)
}
