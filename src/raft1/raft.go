package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Role int
const (
	ROLE_FOLLOWER Role = 0
	ROLE_CANDIDATE Role = 1
	ROLE_LEADER Role = 2
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	replicateInterval time.Duration = 50 * time.Millisecond
	initialLogQueueCapacity int = 1024
)

// an entry of log
type Entry struct {
	Term int
	Index int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	majarity  int                 // the thresold of majarity
	applyCh   chan <- raftapi.ApplyMsg

	// Your data here (3C).

	/// election data
	role            Role
	currentTerm     int
	votedFor        int // -1 means vote for none
	lastSync        time.Time
	electionTimeout time.Duration // random delay

	/// log data and its commit info
	log         []Entry
	commitIndex int
	lastApplied int

	/// state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader= rf.role == ROLE_LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


func (rf *Raft) intoFollower(term int) {
	DPrintf("[term%v node%v Follower] will be follower", term, rf.me)
	rf.role = ROLE_FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = term
}

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int // index of candidate’s last log entry. used for election restriction
	LastLogTerm  int // term of candidate’s last log entry. used for election restriction
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// check stale request
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// new term
	if args.Term > rf.currentTerm {
		rf.intoFollower(args.Term)
		// then processing the request in new term
	}

	// contending or duplicated vote request
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[term%v node%v Follower] refused to vote for peer %vsince i has voted to %v", rf.currentTerm, rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	// check election restriction
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	if args.LastLogTerm < lastLogTerm || 
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex ) {
		DPrintf("[term%v node%v Follower] refused to vote for peer %v since election restriction", rf.currentTerm, rf.me, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 {
		DPrintf("[term%v node%v Follower] grant peer %v vote", rf.currentTerm, rf.me, args.CandidateId)
	} else {
		if rf.votedFor != args.CandidateId {
			panic("rf.votedFor == args.CandidateId")
		}
		DPrintf("[term%v node%v Follower] (duplicated)grant peer %v vote", rf.currentTerm, rf.me, args.CandidateId)
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log)-1
}

func (rf *Raft) getLastLogIndexAndTerm() (index int, term int) {
	index = len(rf.log)-1
	term = rf.log[index].Term
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// source
	Term     int     // leader's term
	LeaderId int     // so follower can redirect clients

	/// used for consistency check
	PrevLogIndex int // index of log entry immediately preceding new ones.
	PrevLogTerm  int // term of prevLogIndex entry.

	/// log data and commit info
	Entries  []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// Check stale request
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// Switch to Follower if newer term found
	if args.Term > rf.currentTerm {
		rf.intoFollower(args.Term)
		// then processing the request in new term
	}

	rf.lastSync = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)

	// Consistency check: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false // leader will retry with smaller log index
		return
	}
	reply.Success = true

	// Discard confliciting entries with leader: If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	nextIndex := args.PrevLogIndex+1
	if len(rf.log) > nextIndex && rf.log[nextIndex].Term != args.Term {
		rf.log = rf.log[:nextIndex]
	}

	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)
	// keep commitIndex latest with leader
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("[term%v node%v Follower] learn commitIndex %v from leader", rf.currentTerm, rf.me, args.LeaderCommit)
		// but we must received the logs before we prepare to commit it
		lastLogIndex := rf.getLastLogIndex()
		if args.LeaderCommit < lastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
		DPrintf("[term%v node%v Follower] update commitIndex to %v", rf.currentTerm, rf.me, rf.commitIndex)
		rf.applyEntries()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// apply all commited but not applied to state machine
func (rf *Raft) applyEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[rf.lastApplied].Command,
			CommandIndex:  rf.lastApplied,
		}
		DPrintf("[node%v applyEntries] apply log %v", rf.me, rf.lastApplied)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = rf.currentTerm
	isLeader = false
    if rf.role != ROLE_LEADER {
		return
	}
	isLeader = true
	index = len(rf.log)
	rf.log = append(rf.log, Entry{
		Term: term,
		Index: index,
		Command: command,
	})
	rf.nextIndex[rf.me]= index + 1
	rf.matchIndex[rf.me]= index
	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.maybeStartElection()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) maybeStartElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Check if a leader election should be started.
	if rf.role != ROLE_LEADER && time.Since(rf.lastSync) > rf.electionTimeout {
		// convert to a candidate
		rf.currentTerm++
		rf.role = ROLE_CANDIDATE
		rf.votedFor = rf.me
		DPrintf("[term%v node%v Candidate] start an election", rf.currentTerm, rf.me,)

		voted := 1 // vote from self
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,

				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			}

			go rf.requestVoteFromPeer(i, args, &voted)
		}

	}
}

// vote is already protected by rf.mu
func (rf *Raft) requestVoteFromPeer(peerIndex int, args *RequestVoteArgs, voted *int) {
	reply := &RequestVoteReply{}
	DPrintf("[term%v node%v Candidate] request peer %v 's vote", args.Term, args.CandidateId, peerIndex)
	ok := rf.sendRequestVote(peerIndex, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		DPrintf("[term%v node%v Candidate] request peer %v 's vote failed", args.Term, args.CandidateId, peerIndex)
		return
	}

	// discover new term
	if reply.Term > rf.currentTerm {
		rf.intoFollower(reply.Term)
		return // stop since no longer candidate
	}

	// check the context: if the node has win the election(so become a ledaer) or no longer candidate(due to newer term found), then stop to request vote
	if rf.role != ROLE_CANDIDATE || rf.currentTerm != args.Term {
		return
	}

	// count the votes
	if reply.VoteGranted {
		DPrintf("[term%v node%v Candidate] got peer %v 's vote", rf.currentTerm, rf.me, peerIndex)
		*voted++
	} else {
		DPrintf("[term%v node%v Candidate] refused by peer %v", rf.currentTerm, rf.me, peerIndex)
	}

	DPrintf("[term%v node%v Candidate] collect %v vote(s)", rf.currentTerm, rf.me, *voted)
	if *voted >= rf.majarity && rf.role == ROLE_CANDIDATE {
		DPrintf("[term%v node%v Candidate] win the election", rf.currentTerm, rf.me)
		rf.role = ROLE_LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		lastLogIndex := rf.getLastLogIndex()
		for i := range rf.nextIndex {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		go rf.replicationTicker(args.Term)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.majarity = (len(rf.peers)+1)/2
	rf.applyCh = applyCh
	// Your initialization code here (3C).
	rf.role = ROLE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 1, initialLogQueueCapacity)
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

// could only replicate in the given term
func (rf *Raft) replicationTicker(term int) {
	DPrintf("[term%v node%v Leader Replication] start replicationTicker", term, rf.me)
	for rf.killed() == false {
		ok := rf.startReplication(term)
		if !ok {
			DPrintf("[term%v node%v Leader Replication] stop replicationTicker", term, rf.me)
			break
		}

		time.Sleep(replicateInterval)
	}
}

func (rf *Raft) startReplication(term int) bool {
	DPrintf("[term%v node%v Leader Replication] start startReplication", term, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[term%v node%v Leader Replication] lock?", term, rf.me)

	if rf.role != ROLE_LEADER || rf.currentTerm != term {
		if rf.role != ROLE_LEADER {
			DPrintf("[term%v node%v Leader Replication] stop because no longer leader", term, rf.me)
		} else {
			DPrintf("[term%v node%v Leader Replication] stop because new term %v", term, rf.me, rf.currentTerm)
		}
		return false
	}

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		nextIndex := rf.nextIndex[peerIdx]
		endIndex := len(rf.log)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex-1,
			PrevLogTerm:  rf.log[nextIndex-1].Term,
			Entries:      rf.log[nextIndex : endIndex],
			LeaderCommit: rf.commitIndex,
		}

		go func(peer int, args *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			DPrintf("[term%v node%v Leader Replication] peer %v sendAppendEntries", args.Term, args.LeaderId, peer)
			ok := rf.sendAppendEntries(peer, args, reply)
			if !ok {
				DPrintf("[term%v node%v Leader Replication] peer %v sendAppendEntries failed", args.Term, args.LeaderId, peer)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != ROLE_LEADER || rf.currentTerm != term {
				DPrintf("[term%v node%v Leader Replication] peer %v stop", term, rf.me, peerIdx)
				return
			}

			// discover newer term
			if reply.Term > rf.currentTerm {
				rf.intoFollower(reply.Term)
				return // stop since no longer leader
			}
			if reply.Success {
				rf.nextIndex[peerIdx] = endIndex
				rf.matchIndex[peerIdx] = endIndex - 1
				DPrintf("[term%v node%v Leader Replication] peer %v AppendEntries Success num_entries:%v nextIndex:%v Entries:%#v", term, rf.me, peer, len(args.Entries), endIndex, args.Entries)
			} else {
				if rf.nextIndex[peerIdx] == 1 {
					// impossible. faulty peer because log 0 must be consistent
					DPrintf("[term%v node%v Leader Replication] faulty peer %v.", term, rf.me, peer)
					return // give up to replicate to this peer
				}
				DPrintf("[term%v node%v Leader Replication] peer %v nextIndex--", term, rf.me, peer)
				rf.nextIndex[peerIdx]--
			}
		}(peerIdx, args)
	}
	// bump commit index in leader if majarity replicated

	for nextCommitIndex := rf.commitIndex+1; nextCommitIndex<len(rf.log); nextCommitIndex++{
		if rf.log[nextCommitIndex].Term != rf.currentTerm {
			continue // only directly commit current term
		}
		// count replication for log[nextCommitIndex]
		numReplica := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= nextCommitIndex {
				numReplica++
			}
		}
		if numReplica >= rf.majarity  {
			rf.commitIndex = nextCommitIndex
			DPrintf("[term%v node%v Leader Replication] set commitIndex to %v", term, rf.me, rf.commitIndex)
		}
	}
	rf.applyEntries()
	return true
}