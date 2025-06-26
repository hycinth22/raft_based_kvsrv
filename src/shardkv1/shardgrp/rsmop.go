package shardgrp

import (
	"6.5840/labgob"
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

type GetOp struct {
	Args *rpc.GetArgs
}

type PutOp struct {
	Args *rpc.PutArgs
}

type FreezeShardOp struct {
	Args *shardrpc.FreezeShardArgs
}

type InstallShardOp struct {
	Args *shardrpc.InstallShardArgs
}

type DeleteShardOp struct {
	Args *shardrpc.DeleteShardArgs
}

func init() {
	labgob.Register(GetOp{})
	labgob.Register(PutOp{})
	labgob.Register(FreezeShardOp{})
	labgob.Register(InstallShardOp{})
	labgob.Register(DeleteShardOp{})
}

func (kv *KVServer) doGet(op GetOp) (reply rpc.GetReply) {
	kv.dlog("[DoGet] %#v", op.Args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := op.Args
	shard, ok := kv.getShardByKey(args.Key)
	if !ok {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	sharddata := shard.Data
	entry, exist := sharddata[args.Key]
	if !exist {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = entry.Value
	reply.Version = entry.Version
	reply.Err = rpc.OK
	return
}

func (kv *KVServer) doPut(op PutOp) (reply rpc.PutReply) {
	kv.dlog("[DoPut] Put %#v", op.Args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := op.Args
	shard, ok := kv.getShardByKey(args.Key)
	if !ok || shard.Freezed {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	sharddata := shard.Data
	entry, exist := sharddata[args.Key]
	if exist {
		// existing key path
		if args.Version != rpc.Tversion(entry.Version) {
			reply.Err = rpc.ErrVersion
			return
		}
		// update existing key
		entry.Value = args.Value
		entry.Version++
		reply.Err = rpc.OK
		return
	} else {
		// new key path
		if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		// create new key
		entry := new(Entry)
		entry.Key = args.Key
		entry.Value = args.Value
		entry.Version = 1
		sharddata[entry.Key] = entry
		reply.Err = rpc.OK
		return
	}
}

func (kv *KVServer) doFreezeShard(op FreezeShardOp) (reply shardrpc.FreezeShardReply) {
	kv.dlog("[doFreezeShard] FreezeShard %#v", op.Args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := op.Args
	shard, ok := kv.getShard(args.Shard)
	reply.Num = shard.ConfigNum
	if !ok {
		reply.Err = rpc.ErrNoShard
		return
	}
	if args.Num < shard.ConfigNum {
		reply.Err = rpc.ErrVersion // stale request
		return
	}
	shard.ConfigNum = args.Num
	shard.Freezed = true
	shardstate := shard.Snapshot()
	reply.State = shardstate
	reply.Err = rpc.OK
	return
}

func (kv *KVServer) doInstallShard(op InstallShardOp) (reply shardrpc.InstallShardReply) {
	kv.dlog("[doInstallShard] InstallShard %#v", op.Args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := op.Args
	shardId := args.Shard
	state := args.State
	shard, ok := kv.makeShard(shardId)
	if Force {
		shard, ok = kv.getShard(shardId)
		if !ok {
			panic("makeShard report shard already exists but getShared failed...")
		}
	} else {
		if !ok {
			kv.dlog("[doFreezeShard] InstallShard already exist%#v", op.Args)
			reply.Err = rpc.ErrExistShard
			return
		}
	}
	if args.Num < shard.ConfigNum {
		reply.Err = rpc.ErrVersion // stale request
		return
	}
	err := shard.Restore(state)
	if err != nil {
		kv.dlog("[doFreezeShard] InstallShard shard.Restore failed %#v", op.Args)
		reply.Err = rpc.ErrInvalidShardState
		return
	}
	shard.Freezed = false
	reply.Err = rpc.OK
	return
}

func (kv *KVServer) doDeleteShard(op DeleteShardOp) (reply shardrpc.DeleteShardReply) {
	kv.dlog("[doDeleteShard] DeleteShard %#v", op.Args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := op.Args
	shard, ok := kv.getShard(args.Shard)
	if !ok {
		if Force {
			reply.Err = rpc.OK
			return
		}
		reply.Err = rpc.ErrNoShard
		return
	}
	if args.Num < shard.ConfigNum {
		reply.Err = rpc.ErrVersion // stale request
		return
	}
	kv.deleteShard(args.Shard)
	reply.Err = rpc.OK
	return
}

