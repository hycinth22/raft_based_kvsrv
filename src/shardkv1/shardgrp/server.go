package shardgrp

import (
	"sync"
	"sync/atomic"
	"strings"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ShardsInitCapacity int = 1024
	SingleShardInitCapacity int = 1024
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu   sync.Mutex
	shards map[ShardId]*Shard
}

type ShardId = shardcfg.Tshid
type Key = string
type Value = string
type ConfigNum = shardcfg.Tnum

type Shard struct {
	Data         map[Key]*Entry
	Freezed      bool
	ConfigNum    ConfigNum // greatest version seen
}

type Entry struct {
	Key     Key
	Value   Value
	Version rpc.Tversion // 0 for non-exist. 1 is least valid version
}

func (kv *KVServer) DoOp(req any) any {
	if op, ok := req.(GetOp); ok {
		return kv.doGet(op)
	} else if op, ok := req.(PutOp); ok {
		return kv.doPut(op)
	} else if op, ok := req.(FreezeShardOp); ok {
		return kv.doFreezeShard(op)
	} else if op, ok := req.(InstallShardOp); ok {
		return kv.doInstallShard(op)
	} else if op, ok := req.(DeleteShardOp); ok {
		return kv.doDeleteShard(op)
	}
	return nil
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	op := GetOp{
		args,
	}
	kv.dlog("[Get] submit %#v", op.Args)
	err, doResult := kv.rsm.Submit(op)
	kv.dlog("[Get] doResult err %v doResult %#v", err, doResult)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = doResult.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	op := PutOp{
		args,
	}
	kv.dlog("[Put] submit %#v", op.Args)
	err, doResult := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	kv.dlog("[Put] doResult err %v doResult %#v", err, doResult)
	*reply = doResult.(rpc.PutReply)
}

func (kv *KVServer) getShardByKey(key Key) (*Shard, bool) {
	shardId := shardcfg.Key2Shard(key)
	shard, ok := kv.shards[shardId]
	return shard, ok
}

func (kv *KVServer) getShard(shardId ShardId) (*Shard, bool) {
	shard, ok := kv.shards[shardId]
	return shard, ok
}

func (kv *KVServer) makeShard(shardId ShardId) (*Shard, bool) {
	_, ok := kv.shards[shardId]
	if ok {
		return nil, false
	}
	kv.shards[shardId] = new(Shard)
	return kv.shards[shardId], true
}

func (kv *KVServer) deleteShard(shardId ShardId) {
	delete(kv.shards, shardId)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	op := FreezeShardOp{
		args,
	}
	kv.dlog("[FreezeShard] submit %#v", op.Args)
	err, doResult := kv.rsm.Submit(op)
	kv.dlog("[FreezeShard] doResult err %v doResult %#v", err, doResult)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = doResult.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	op := InstallShardOp{
		args,
	}
	kv.dlog("[InstallShard] submit %#v", op.Args)
	err, doResult := kv.rsm.Submit(op)
	kv.dlog("[InstallShard] doResult err %v doResult %#v", err, doResult)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = doResult.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	op := DeleteShardOp{
		args,
	}
	kv.dlog("[DeleteShard] submit %#v", op.Args)
	err, doResult := kv.rsm.Submit(op)
	kv.dlog("[DeleteShard] doResult err %v doResult %#v", err, doResult)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = doResult.(shardrpc.DeleteShardReply)
}

// calls Kill() when a KVServer instance won't be needed again. 
// it may be convenient (for example) to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.shards = make(map[ShardId]*Shard, ShardsInitCapacity)
	if len(kv.shards) == 0 && gid == 1 {
		for shardId := shardcfg.Tshid(0); shardId<shardcfg.NShards; shardId++ {
			shard, _ := kv.makeShard(shardId)
			shard.Data = make(map[Key]*Entry, SingleShardInitCapacity)
		}
	}
	kv.dlog("StartKVServer %#v", kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}

func (kv *KVServer) dlog(format string, a ...interface{}) {
	dataformat := func() string {
		// kv.mu.Lock()
		// defer kv.mu.Unlock()
		var b strings.Builder
		// for sid := range kv.shards {
		// 	shard := kv.shards[sid]
		// 	for key := range shard.Data {
		// 		fmt.Fprintf(&b, "[%v] %#v", key, shard.Data[key])
		// 	}
		// }

		return b.String()
	}
	args := []any{
		kv.gid,
		kv.me,
		dataformat(),
	}
	args = append(args, a...)
	DPrintf("[KVServer group%v node%v %#v] " + format, args...)
}
