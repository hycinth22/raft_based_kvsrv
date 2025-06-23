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

	waitingOpIndex atomic.Int64

	condApply      *sync.Cond
	appliedOpIndex int64
	applyResult    *OpResult

	closed         atomic.Bool
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

		condApply:      sync.NewCond(&sync.Mutex{}),
		appliedOpIndex: 0,
	}
	rsm.waitingOpIndex.Store(-1)
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
	// generate op and assign unique ReqId to it
	reqId := int(time.Now().UnixNano())
	op := Op{Submitter: rsm.me, ReqId: reqId, Req: req}

	// submit to raft
	rsm.dlog("[op %v] Start to submit %#v", op.ReqId, op)
	index, startTerm, ok := rsm.rf.Start(op)
	if !ok {
		rsm.dlog("submit fail because it's not the leader")
		return rpc.ErrWrongLeader, nil // i'm not leader, try another server.
	}
	rsm.dlog("start to submit in the node index %d term %d", index, startTerm)

	// make sure reqId unique for next submit
	for int(time.Now().UnixNano()) == reqId {
		time.Sleep(1 * time.Nanosecond)
	}

	rsm.waitingOpIndex.Store(int64(index))
	rsm.dlog("[op %v] Submit waitingOpIndex %v", op, index)

	// checking our request is commited in raft...
	done := make(chan bool, 2)
	go func() {
		rsm.condApply.L.Lock()
		defer rsm.condApply.L.Unlock()
		rsm.dlog("[op %v] Submit first checking...", op)
		for !rsm.closed.Load() && rsm.appliedOpIndex < int64(index){
			rsm.dlog("[op %v] Submit condApply.Wait...", op)
			rsm.condApply.Wait()
			rsm.dlog("[op %v] Submit condApply.Wait done", op)
		}
		done <- true
	}()
	go func() {
		for {
			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader || currentTerm != startTerm {
				done <- false
				return // i'm no longer leader, the result is unkonwn
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	<- done
	if rsm.closed.Load() || rsm.applyResult == nil {
		rsm.dlog("[op %v] submit result ErrMaybe.", op)
		return rpc.ErrMaybe, nil // i'm dead, the result is unkonwn
	}
	rsm.dlog("[op %v] Submit ready to produce result", op)
	//  submited but failed on agreement... different entry appears on this index
	if rsm.appliedOpIndex > int64(index) || rsm.applyResult.Submitter != rsm.me {
		rsm.dlog("[op %v] Submit ErrWrongLeader", op)
		return rpc.ErrWrongLeader, nil
	}
	// rsm.appliedOpIndex == index, rsm.applyResult.Submitter == rsm.me
	// so our req is applied successfully
	if op.ReqId != reqId {
		panic("op.ReqId != reqId") // something wrong
	}
	opResult := rsm.applyResult
	rsm.applyResult = nil
	result := opResult.Result
	rsm.dlog("[op %v] submit successfully. apply op %#v result %#v", op.ReqId, op, result)
	return rpc.OK, result

	// // our request maybe lost because fail of agreement
	// currentTerm, isLeader := rsm.rf.GetState()
	// if !isLeader || currentTerm != startTerm {
	// 	rsm.dlog("[op %v] submit result ErrWrongLeader.")
	// 	return rpc.ErrWrongLeader, nil // i'm not leader, try another server.
	// }
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			func() {
				rsm.condApply.L.Lock()
				defer rsm.condApply.L.Unlock()
				rsm.appliedOpIndex++
				op := msg.Command.(Op)
				rsm.dlog("apply op %#v", op)
				result := rsm.sm.DoOp(op.Req)
				opResult := &OpResult{
					Submitter: op.Submitter,
					ReqId:     op.ReqId,
					Result:    result,
				}
				rsm.dlog("apply op result %#v %p", result, rsm)

				// not all op which op.Commiter=rsm.me need to be pass via rsm.applyResult
				// because its may be replaying(crash or others)... so no Submit() is waiting for it
				// check the situation using rsm.waitingOpIndex
				if rsm.appliedOpIndex == rsm.waitingOpIndex.Load() {
					// pass the apply result
					rsm.applyResult = opResult
					// wakes all waiting Submit().
					// one will consume the applyResult and return success
					// older will see they be outdated and return error
					// newer still wait
					rsm.dlog("condApply.Broadcast for op %v result%#v", op, opResult)
					rsm.condApply.L.Unlock()
					rsm.condApply.Broadcast()
					rsm.condApply.L.Lock()
					rsm.dlog("condApply.Broadcast over")
				}
			}()

		} else if msg.SnapshotValid {
			panic("snapshot unsupported")
		}
	}
	rsm.closed.Store(true)
	rsm.condApply.Broadcast() // notify the closing
}


func (rsm *RSM) dlog(format string, a ...interface{}) {
	args := []any{
		rsm.me,
		rsm.waitingOpIndex.Load(),
		rsm.appliedOpIndex,
	}
	args = append(args, a...)
	DPrintf("[rsm %v waiting %v applied %v] " + format, args...)
}
