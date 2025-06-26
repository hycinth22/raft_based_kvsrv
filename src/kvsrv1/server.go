package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Key = string

type Value struct {
	value string
	version uint64
}

type KVServer struct {
	mu sync.Mutex

	rpcsrv *labrpc.Server
	kvpairs map[Key]Value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kvpairs = make(map[Key]Value)
	rpcsrv := labrpc.MakeServer()
	rpcsrv.AddService(labrpc.MakeService(kv))
	kv.rpcsrv = rpcsrv
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist := kv.kvpairs[args.Key]
	if !exist {
		reply.Err = rpc.ErrNoKey
		return
	}
	*reply = rpc.GetReply {
		Value: value.value,
		Version: rpc.Tversion(value.version),
		Err: rpc.OK,
	}
	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldvalue, exist := kv.kvpairs[args.Key]
	if exist {
		// existing key path
		if args.Version != rpc.Tversion(oldvalue.version) {
			reply.Err = rpc.ErrVersion
			return
		}
		// update existing key
		kv.kvpairs[args.Key] = Value {
			value: args.Value,
			version: oldvalue.version+1,
		}
		*reply = rpc.PutReply {
			Err: rpc.OK,
		}
	} else {
		// new key path
		if args.Version > 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		// create new key
		kv.kvpairs[args.Key] = Value {
			value: args.Value,
			version: 1,
		}
		*reply = rpc.PutReply {
			Err: rpc.OK,
		}
	}
	return
}

func (kv *KVServer) Kill() {
}

func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
