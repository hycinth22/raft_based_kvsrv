package rsm

import (
	"time"
	"sync"
	"sync/atomic"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"

)

var useRaftStateMachine bool // to plug in another raft besided raft1


type Op struct {
	Submitter int
	ReqId     int
	Req       any
}

type OpResult struct {
	Submitter  int
	ReqId      int
	Result     any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	resultCh     map[int]chan OpResult // pass the result on sm from reader to Submit(). index is the requst id
	closed       atomic.Bool
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		resultCh:     make(map[int]chan OpResult),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	reqId := int(time.Now().UnixNano())
	op := Op{Submitter: rsm.me, ReqId: reqId, Req: req}
	rsm.resultCh[reqId] = make(chan OpResult, 1)

	// submit to raft
	rsm.dlog("Start %#v", op)
	index, startTerm, ok := rsm.rf.Start(op)
	if !ok {
		rsm.dlog("submit fail because it's not the leader")
		return rpc.ErrWrongLeader, nil // i'm not leader, try another server.
	}
	rsm.dlog("submit in the node index %d term %d", index, startTerm)

	// make sure reqId unique for next submit
	for int(time.Now().UnixNano()) == reqId {
		time.Sleep(1 * time.Nanosecond)
	}

	// checking our request is commited in raft...
	for {
		select {
			case opResult := <- rsm.resultCh[reqId] :
				result := opResult.Result
				rsm.dlog("receive apply op %#v result %#v", op, result)

				close(rsm.resultCh[reqId])
				rsm.resultCh[reqId] = nil
				return rpc.OK, result
			default:
				// our request maybe lost because fail of agreement
				currentTerm, isLeader := rsm.rf.GetState()
				if !isLeader || currentTerm != startTerm {
					return rpc.ErrWrongLeader, nil // i'm not leader, try another server.
				}

				if rsm.closed.Load() {
					return rpc.ErrMaybe, nil // i'm dead, the result is unkonwn
				}
		}
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			rsm.dlog("apply op %#v", op)
			result := rsm.sm.DoOp(op.Req)
			opResult := OpResult{
				Submitter: op.Submitter,
				ReqId:     op.ReqId,
				Result:    result,
			}
			rsm.dlog("apply op result %#v", result)
			// if submmiter is other server, our client is not wating for it...
			if op.Submitter == rsm.me {
				// if the request is a duplicated apply(due to crash or restart), resultCh is nil, no consumer
				// and we should ignore it, otherwise we will deadlock for waiting on nil
				if rsm.resultCh[op.ReqId] != nil {
					rsm.resultCh[op.ReqId] <- opResult
				}
			}
		} else if msg.SnapshotValid {
			panic("snapshot unsupported")
		}
	}
	rsm.closed.Store(true)
}


func (rsm *RSM) dlog(format string, a ...interface{}) {
	args := []any{
		rsm.me,
	}
	args = append(args, a...)
	DPrintf("[rsm %v] " + format, args...)
}
