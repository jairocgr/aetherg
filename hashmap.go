package main

import (
	"strconv"
	"time"
)

type hashmap struct {
	data          map[string]*item
	transientKeys map[string]bool
	dirty         bool
}

type item struct {
	key        string
	value      []byte
	expiration int
	creation   time.Time
}

func newHashmap() *hashmap {
	hm := new(hashmap)
	hm.data = make(map[string]*item)
	hm.transientKeys = make(map[string]bool)
	return hm
}

func (hm *hashmap) get(key string) (*item, bool) {
	i, ok := hm.data[key]
	return i, ok
}

func (hm *hashmap) rm(key string) {
	delete(hm.data, key)
	delete(hm.transientKeys, key)
	hm.dirty = true
}

func (hm *hashmap) set(key string, val []byte, expiration int) {
	hm.data[key] = &item{
		key:        key,
		value:      val,
		expiration: expiration,
		creation:   time.Now(),
	}

	hm.dirty = true

	if expiration != 0 {
		hm.transientKeys[key] = true
	}
}

func (hm *hashmap) getKeys() []string {
	keys := make([]string, 0)
	for key := range hm.data {
		keys = append(keys, key)
	}
	return keys
}

func (hm *hashmap) count() int {
	return len(hm.data)
}

func (hm *hashmap) rmall() {
	hm.data = make(map[string]*item)
	hm.transientKeys = make(map[string]bool)
	hm.dirty = true
}

func (hm *hashmap) getItens() []*item {
	itens := []*item{}
	for _, item := range hm.data {
		itens = append(itens, item)
	}
	return itens
}

func (hm *hashmap) evict() {
	for key := range hm.transientKeys {
		item, _ := hm.get(key)
		if item.isTransient() && item.hasExpired() {
			hm.rm(key)
		}
	}
}

func (hm *hashmap) isDirty() bool {
	return hm.dirty
}

func (hm *hashmap) washClean() {
	hm.dirty = false
}

func (i *item) getValue() []byte {
	return i.value
}

func (i *item) toString() string {
	return string(i.value)
}

func (i *item) getKey() string {
	return i.key
}

func (i *item) getExpiration() int {
	return i.expiration
}

func (i *item) getCreation() time.Time {
	return i.creation
}

func (i *item) isTransient() bool {
	return i.expiration != 0
}

func (i *item) getTimeToLive() int {
	return i.expiration - i.getAge()
}

func (i *item) getAge() int {
	now := time.Now()
	age := now.Unix() - i.creation.Unix()
	return int(age)
}

func (i *item) hasExpired() bool {
	return i.getTimeToLive() <= 0
}

func (i *item) genSetCommandPieces() [][]byte {
	pieces := [][]byte{
		[]byte("SET"),
		[]byte(i.getKey()),
		i.getValue(),
	}

	if i.isTransient() {
		// TODO: Sending expiration to the replica as the current TTL is a dangerous and simplistic assumption
		// It assumes that the lastMeasurement between now and the lastMeasurement that this item settle in the replica's hashmap, it passes
		// ZERO seconds. A better replication algorithm would take this into consideration.
		ttl := i.getTimeToLive()
		expiration := strconv.Itoa(ttl)
		pieces = append(pieces, []byte("EXP"))
		pieces = append(pieces, []byte(expiration))
	}

	return pieces
}
