package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	return ck
}


// Get a key from a shardgrp.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shardId := key2shardId(key)
	sgck := ck.makeShardGroupClerk(shardId)
	val, ver, err := sgck.Get(key)
	for err == rpc.ErrWrongGroup || err == rpc.ErrgGroupMaybeLeave {
		log.Println("Get retry for err:", err)
		sgck = ck.makeShardGroupClerk(shardId)
		val, ver, err = sgck.Get(key)
	}
	return val, ver, err
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shardId := key2shardId(key)
	sgck := ck.makeShardGroupClerk(shardId)
	err := sgck.Put(key, value, version)
	for err == rpc.ErrWrongGroup || err == rpc.ErrgGroupMaybeLeave {
		log.Println("Put retry for err:", err)
		sgck = ck.makeShardGroupClerk(shardId)
		err = sgck.Put(key, value, version)
		// see explain in sgck.Put()
		if err == rpc.ErrVersion {
			err = rpc.ErrMaybe
		}
	}
	return err
}

func key2shardId(key string) shardcfg.Tshid {
	shardId := shardcfg.Key2Shard(key)
	return shardId
}

func (ck *Clerk) makeShardGroupClerk(shardId  shardcfg.Tshid) *shardgrp.Clerk {
	config := ck.sck.Query()
	gid, srvs, ok := config.GidServers(shardId)
	log.Println("makeShardGroupClerk: shardId ", shardId, " on gid", gid)
	if !ok {
		log.Println("GidServers failed. invalid gid?", gid)
		panic("GidServers failed. invalid gid?")
	}
	sgck := shardgrp.MakeClerk(ck.clnt, srvs)
	return sgck
}