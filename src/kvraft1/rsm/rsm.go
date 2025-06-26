package rsm

import (
	"time"
	"sync"
	"sync/atomic"
	"runtime"
	"bytes"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
	"6.5840/labgob"
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

func init() {
	labgob.Register(Op{})
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
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	mu           sync.Mutex

	waitingOpReqId chan int

	waitingApplyReq atomic.Int64  // only for dlog  now
	appliedOpIndex  atomic.Int64  // only for dlog and sanity checking now

	applyResult    sync.Map      // map[int]chan OpResult

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
	}
	rsm.waitingOpReqId = make(chan int, 1024)
	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		rsm.readSnapshot(snapshot)
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	rsm.dlog("start up")
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
// ErrWrongLeader if no longer Leader
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	var op Op

	// submit to raft
	submitReq := func(req any) (bool, int, int) {
		rsm.mu.Lock()
		defer rsm.mu.Unlock()

		// generate op and assign unique ReqId to it
		reqId := int(time.Now().UnixNano())
		op = Op{Submitter: rsm.me, ReqId: reqId, Req: req}

		// submit to raft
		rsm.dlog("[op %v] Start to submit %#v", op.ReqId, op)
		index, term, ok := rsm.rf.Start(op)
		if !ok {
			rsm.dlog("submit fail because it's not the leader")
			return false, index, term
		}
		rsm.dlog("start to submit in the node index %d term %d", index, term)

		rsm.dlog("[op %v %v] Submit waitingOpIndex %v", index, op, index)

		rsm.applyResult.Store(index, make(chan OpResult, 1))

		// make sure reqId unique for next submit
		for int(time.Now().UnixNano()) == reqId {
			time.Sleep(1 * time.Nanosecond)
		}
		return true, index, term
	}

	// checking our request is commited in raft...
	waitForCommited := func(index int, term int) (rpc.Err, any) {
		var opResult *OpResult

		noLongerLeader := make(chan struct{}, 1)
		go func() {
			startTime := time.Now()
			for !rsm.closed.Load() && time.Since(startTime) < 2000 * time.Millisecond {
				currentTerm, isLeader := rsm.rf.GetState()
				if !isLeader || currentTerm != term {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			noLongerLeader <- struct{}{}
		}()
		applyResultCh, ok := rsm.applyResult.Load(index)
		if !ok {
			panic("applyResult[index] should already set in submitReq")
		}
		select {
		case applyResult := <- applyResultCh.(chan OpResult):
			opResult = &applyResult
		case <- noLongerLeader:
			return rpc.ErrWrongLeader, nil
		}

		rsm.dlog("[op %v %v] Submit ready to produce result", index, op)

		rsm.applyResult.Delete(index)

		if opResult == nil {
			return rpc.ErrWrongLeader, nil  // i'm dead
		}

		if opResult.Submitter != rsm.me {
			// submited but failed on agreement... different entry appears on this index
			rsm.dlog("[op %v %v] Submit ErrWrongLeader", index, op)
			return rpc.ErrWrongLeader, nil
		}

		// our req is applied successfully
		if opResult.ReqId != op.ReqId {
			panic("opResult.ReqId != op.ReqId") // something wrong
		}
		result := opResult.Result
		rsm.dlog("[op %v %v] submit successfully. apply op %#v result %#v", index, op.ReqId, op, result)
		return rpc.OK, result

	}

	ok, index, term := submitReq(req)
	if  !ok {
		return rpc.ErrWrongLeader, nil // i'm not leader, try another server.
	}
	return waitForCommited(index, term)
}

func (rsm *RSM) reader() {
	waitingOpReqId := -1
	updateWaitingOpReqId := func() bool {
		rsm.dlog("enter waitingOpReqId")
		select{
		case waitingOpReqId = <- rsm.waitingOpReqId:
			rsm.waitingApplyReq.Store(int64(waitingOpReqId))
			rsm.dlog("com waitingOpReqId %v", waitingOpReqId)
			return true // ok
		default:
			rsm.dlog("fail waitingOpReqId")
			return false // donothing
		}
	}
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			func() {
				rsm.dlog("receive apply msg %#v", msg)
				index := msg.CommandIndex

				appliedOpIndex := rsm.appliedOpIndex.Add(1)
				if appliedOpIndex != int64(index) {
					rsm.dlog("wrong command index %v. something preceding not applied appliedOpIndex %v", index, appliedOpIndex)
					panic("wrong command index. something preceding not applied")
				}

				op := msg.Command.(Op)
				rsm.dlog("apply op %v %#v", index, op)
				result := rsm.sm.DoOp(op.Req)
				opResult := OpResult{
					Submitter: op.Submitter,
					ReqId:     op.ReqId,
					Result:    result,
				}
				rsm.dlog("apply op %v result %#v %p", index, result, rsm)

				if waitingOpReqId == -1 {
					updateWaitingOpReqId()
				}
				applyResultCh, ok := rsm.applyResult.Load(index)
				// not all op although op.Commiter=rsm.me need to be pass via rsm.applyResult
				// because its may be replaying(crash or others)... so no Submit() is waiting for it
				// check the situation via checking applyResultCh existing
				// if waitingOpReqId == op.ReqId but !ok, it says submit but applyResult not set yet, wait it
				for waitingOpReqId == op.ReqId && !ok {
					applyResultCh, ok = rsm.applyResult.Load(index)
					runtime.Gosched()
				}
				if ok {
					// pass the apply result
					rsm.dlog("send apply op %v result %#v %p", index, result, rsm)
					applyResultCh.(chan OpResult) <- opResult
					rsm.dlog("sent apply op %v result %#v %p", index, result, rsm)
				}

				rsm.snapshotIfNeed(index)
			}()
		} else if msg.SnapshotValid {
			rsm.dlog("receive apply msg %#v", msg)
			rsm.readSnapshot(msg.Snapshot)
			applied := rsm.appliedOpIndex.Load()
			for applied < int64(msg.SnapshotIndex) {
				// update
				ok := rsm.appliedOpIndex.CompareAndSwap(applied, int64(msg.SnapshotIndex))
				if ok {
					break
				}
			}
		}
	}
	rsm.closed.Store(true)
}


type snapshot struct {
	StateMachineSnapshot []byte
	AppliedOpIndex       int64
}

func init() {
	labgob.Register(snapshot{})
}

func (rsm *RSM) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot snapshot
	if d.Decode(&snapshot) != nil {
		return
	}

	if snapshot.StateMachineSnapshot != nil && len(snapshot.StateMachineSnapshot) > 0 {
		rsm.sm.Restore(snapshot.StateMachineSnapshot) // notify application-layer to restore
	}

	applied := rsm.appliedOpIndex.Load()
	for applied < int64(snapshot.AppliedOpIndex) {
		// update
		ok := rsm.appliedOpIndex.CompareAndSwap(applied, int64(snapshot.AppliedOpIndex))
		if ok {
			break
		}
	}
}

func (rsm *RSM) generateSnapshot() []byte {
	smSnapshot := rsm.sm.Snapshot() // request application-layer snapshot

	// append rsm snapshot metadata
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot := snapshot{
		StateMachineSnapshot: smSnapshot,
		AppliedOpIndex:       rsm.appliedOpIndex.Load(),
	}
	e.Encode(snapshot)
	return w.Bytes()
}

func (rsm *RSM) snapshotIfNeed(index int) {
	// snapshot if rf.PersistBytes() > maxraftstate
	if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
		snapshot := rsm.generateSnapshot()
		rsm.rf.Snapshot(index, snapshot) // notify underlying-layer
	}
}

func (rsm *RSM) dlog(format string, a ...interface{}) {
	args := []any{
		rsm.me,
		rsm.waitingApplyReq.Load(),
		rsm.appliedOpIndex.Load(),
	}
	args = append(args, a...)
	DPrintf("[rsm %v waitingApplyReq %v applied %v] " + format, args...)
}
