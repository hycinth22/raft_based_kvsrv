package shardgrp

import (
	"bytes"

	"6.5840/labgob"
)

type snapshot struct {
	Shards map[ShardId]*Shard
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := snapshot{
		Shards: kv.shards,
	}
	e.Encode(snapshot)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot snapshot
	if d.Decode(&snapshot) != nil {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.shards = snapshot.Shards
}

func (sd *Shard) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sd)
	return w.Bytes()
}

// sd must be zero value
func (sd *Shard) Restore(data []byte) error {
	r := bytes.NewBuffer(data)
	var s Shard
	d := labgob.NewDecoder(r)
	if err := d.Decode(&s); err != nil {
		return err
	}
	*sd = s
	return nil
}