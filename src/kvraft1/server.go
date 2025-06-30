package kvraft

import (
	"sync"
	"sync/atomic"
	"strings"
	"fmt"
	"bytes"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"

)

const (
	DLOG_DATA         bool = true
	KVInitialCapacity int = 1024
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	mu   sync.Mutex
	data map[Key]*Entry
}

type Key = string
type Value = string

type Entry struct {
	Key     Key
	Value   Value
	Version rpc.Tversion // 0 for non-exist. 1 is least valid version
}

type GetOp struct {
	Args *rpc.GetArgs
}

type PutOp struct {
	Args *rpc.PutArgs
}

func init() {
	// rpc including raft types
	labgob.Register(GetOp{})
	labgob.Register(PutOp{})
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	// snapshot types
	labgob.Register(snapshot{})
	labgob.Register(Entry{})
}

func (kv *KVServer) DoOp(req any) any {
	if op, ok := req.(GetOp); ok {
		return kv.doGet(op)
	} else if op, ok := req.(PutOp); ok {
		return kv.doPut(op)
	}
	return nil
}

func (kv *KVServer) doGet(op GetOp) (reply rpc.GetReply) {
	kv.dlog("[DoGet] %#v", op.Args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := op.Args
	entry, exist := kv.data[args.Key]
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
	entry, exist := kv.data[args.Key]
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
		kv.data[entry.Key] = entry
		reply.Err = rpc.OK
		return
	}
}


type snapshot struct {
	Data map[Key]*Entry
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	snapshot := snapshot{
		Data: kv.data,
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
	kv.data = snapshot.Data
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

// calls Kill() when a KVServer instance won't be needed again. 
// it may be convenient (for example) to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	kv := &KVServer{me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.data = make(map[Key]*Entry, KVInitialCapacity)
	kv.dlog("StartKVServer %#v", kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}


func (kv *KVServer) dlog(format string, a ...interface{}) {
	dataformat := func() string {
		var b strings.Builder
		if DLOG_DATA {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			fmt.Fprintln(&b, "{")
			for key := range kv.data {
				fmt.Fprintf(&b, "[%v] %#v", key, kv.data[key])
			}
			fmt.Fprintln(&b, "}")
		}
		return b.String()
	}
	args := []any{
		kv.me,
		dataformat(),
	}
	args = append(args, a...)
	DPrintf("[KVServer %v %v, %#v] " + format, args...)
}
