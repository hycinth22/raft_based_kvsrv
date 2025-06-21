package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

// requirement:
// replicateInterval <<< electionTimeoutMin < electionTimeoutMax
// requestRetryInterval < electionTimeoutMin/2
const (
	electionTimeoutMin time.Duration = 160 * time.Millisecond
	electionTimeoutMax time.Duration = 600 * time.Millisecond

	replicateInterval time.Duration = 50 * time.Millisecond
	initialLogQueueCapacity int = 1024

	OPT_BACK_UP_INCONSITENT_IN_TERM bool = true
)

// an entry of log
type Entry struct {
	Index    int
	Term     int
	Command  interface{}
}

type Snapshot struct {
	lastSnapshotIndex int    // included index in this snapshot
	lastSnapshotTerm  int    // included term in this snapshot
	data              []byte // snapshot. persisted
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

	/// election data
	role            Role
	currentTerm     int  // term of leader. persisted to avoid following a superseded leader or voting in a superseded election.
	votedFor        int  // vote who in the term. -1 means vote for none. persisted to prevent a client from voting for one candidate, then reboot, then vote for a different candidate in the same term could lead to two leaders for the same term
	lastSync        time.Time
	electionTimeout time.Duration // random delay

	/// log data and its commit info
	muLog       sync.Mutex // Lock to protect shared access to lLog
	log         []Entry    // log. . note: index != Entry.Index. persisted: if a server was in leader's majority for committing an entry, must remember entry despite reboot, so next leader's vote majority includes the entry, so Election Restriction ensuresnew leader also has the entry.
	commitIndex int        // note: it's not a array index
	lastApplied int

	/// snapshot
	snapshot Snapshot // snapshot. persisted

	/// state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1). note: it's not a array index
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically) note: it's not a array index
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

type RaftPersistState struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entry

	LastSnapshotIndex int    // included index in this snapshot
	LastSnapshotTerm  int    // included term in this snapshot
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	state := RaftPersistState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,

		LastSnapshotIndex: rf.snapshot.lastSnapshotIndex,
		LastSnapshotTerm:  rf.snapshot.lastSnapshotTerm,
	}
	e.Encode(state)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot.data)
	DPrintf("[node%v persist] state %#v", rf.me, state)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state RaftPersistState
	if d.Decode(&state) != nil {
		return
	}
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
	rf.snapshot.lastSnapshotIndex = state.LastSnapshotIndex
	rf.snapshot.lastSnapshotTerm = state.LastSnapshotTerm
	rf.snapshot.data = make([]byte, len(snapshot))
	rf.snapshot.data = snapshot
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
	if snapshot == nil {
		panic("snapshot is nil")
	}
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if index <= rf.snapshot.lastSnapshotIndex {
			return
		}
		if index < rf.commitIndex {
			rf.commitIndex = index
			// panic("client snapshot include uncommited entry")
		}
		DPrintf("[term%v node%v Snapshot] Snapshot %v", rf.currentTerm, rf.me, index)
		entry := rf.lookupEntryByIndex(index)
		if entry.Index != index {
			panic("Snapshot: entry.Index != index")
		}
		rf.discardEntriesUntil(index)
		rf.snapshot = Snapshot{
			data: snapshot,
			lastSnapshotIndex: index,
			lastSnapshotTerm: entry.Term,
		}
		rf.persist()
		if rf.lastApplied < index {
			rf.lastApplied = index
		}
	}()
}

// return from leader or candidate state
// or simply follower update its term learned from peers
// lock rf.mu before call this
func (rf *Raft) intoFollower(term int) {
	if term < rf.currentTerm  {
		panic("term should be higher(new election) or equal(contending election)")
	}
	DPrintf("[term%v node%v Follower] will be follower", term, rf.me)
	rf.role = ROLE_FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int

	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	Term        int
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if args.Data == nil {
		panic("InstallSnapshot data cannot be nil")
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// check stale request
	if args.Term < rf.currentTerm {
		return
	}
	// new term
	if args.Term > rf.currentTerm {
		rf.intoFollower(args.Term)
		// then processing the request in new term
	}
	DPrintf("[term%v node%v Follower] InstallSnapshot %#v", rf.currentTerm, rf.me, args)
	DPrintf("[term%v node%v Follower] snapshot %#v", rf.currentTerm, rf.me, rf.snapshot)
	DPrintf("[term%v node%v Follower] log %#v", rf.currentTerm, rf.me, rf.log)

	// new index log inclued or inconsistent with leader snapsoht
	if args.LastIncludedIndex >= rf.getLastLogIndex() || rf.lookupEntryByIndex(args.LastIncludedIndex).Term != args.LastIncludedTerm {
		// all should be purged
		rf.discardAllEntries()
	} else {
		// the snapshot is a prefix of the follower's log
		// the remain entries is after snapshot.
		rf.discardEntriesUntil(args.LastIncludedIndex)
	}
	rf.snapshot = Snapshot{
		data:              args.Data,
		lastSnapshotIndex: args.LastIncludedIndex,
		lastSnapshotTerm:  args.LastIncludedTerm,
	}
	rf.persist()
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	// the entries has been applied and transmit via snapshot
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	// rf.nextIndex & rf.matchIndex is unnecessary because the node is follower instead of leader
	// them will be reinitilized when becoming leader
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term        int
	CandidateId int

	LastLogIndex int // index of candidate’s last log entry. used for election restriction
	LastLogTerm  int // term of candidate’s last log entry. used for election restriction
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
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

	// rf.lastSync = time.Now()
	// randRange := int64(electionTimeoutMax - electionTimeoutMin)
	// rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)

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
	rf.persist()
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.snapshot.lastSnapshotIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogIndexAndTerm() (index int, term int) {
	if len(rf.log) == 0 {
		index = rf.snapshot.lastSnapshotIndex
		term = rf.snapshot.lastSnapshotTerm
		return
	}
	index = rf.log[len(rf.log)-1].Index
	term = rf.log[len(rf.log)-1].Term
	return
}

func (rf *Raft) lookupEntryByIndex(index int) *Entry {
	if index == rf.snapshot.lastSnapshotIndex {
		// included in snapshot already, but maybe need it's term...
		return &Entry{
			Term:  rf.snapshot.lastSnapshotTerm,
			Index: rf.snapshot.lastSnapshotIndex,
			Command: nil, // dummy data
		}
	}
	return &rf.log[rf.lookupEntryPosByIndex(index)]
}

// inclusive range [beginIndex, endIndex]
func (rf *Raft) lookupEntriesByIndex(beginIndex int, endIndex int) []Entry {
	if beginIndex > endIndex {
		return []Entry{}
	}
	beginPos := rf.lookupEntryPosByIndex(beginIndex)
	endPos := rf.lookupEntryPosByIndex(endIndex)
	return rf.log[beginPos : endPos+1]
}

func (rf *Raft) lookupEntryPosByIndex(index int) (pos int) {
	if index <= rf.snapshot.lastSnapshotIndex {
		panic("entry cleaned")
	}
	pos = index - (rf.snapshot.lastSnapshotIndex + 1)
	DPrintf("[term%v node%v] lookupEntryPosByIndex %v %v", rf.currentTerm, rf.me, index, pos)
	return
}

// discard [start, until]
func (rf *Raft) discardEntriesUntil(until int) {
	if until <= rf.snapshot.lastSnapshotIndex {
		return
	}
	DPrintf("[term%v node%v] discardEntriesUntil %v %#v", rf.currentTerm, rf.me, until, rf.log)
	pos := rf.lookupEntryPosByIndex(until)
	rf.log = rf.log[pos+1:]
}

// discard [fromIndex, end)
func (rf *Raft) discardEntriesFrom(fromIndex int) {
	DPrintf("[term%v node%v] discardEntriesFrom %v", rf.currentTerm, rf.me, fromIndex)
	pos := rf.lookupEntryPosByIndex(fromIndex)
	rf.log = rf.log[:pos]
}

// discard all
func (rf *Raft) discardAllEntries() {
	DPrintf("[term%v node%v] discardAllEntries", rf.currentTerm, rf.me)
	rf.log = make([]Entry, 0, initialLogQueueCapacity)
}

// send a RequestVote RPC to a server.
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

	XTerm         int // term in the conflicting entry (if any)
	XIndex        int // index of first entry with that term (if any)
	XNextIndex    int // next append index. used only if follower has shorter than args.PrevLogIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	DPrintf("[term%v node%v Follower] AppendEntriesArgs %#v", rf.currentTerm, rf.me, args)

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

	lastLogIndex := rf.getLastLogIndex()

	// Consistency check: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	//
	// During normal operation, the logs of the leader and followers stay consistent, so the AppendEntries consistency check never fails.
	// However, leader crashes can leave the logs inconsistent (the old leader may not have fully replicated all of the entries in its log).
	// A folllower be missing entries that are present on the leader, it may have extra entries that are not present on the leader, or both.
	// Once failed, leader will retry with smaller PrevLogIndex
	if args.PrevLogIndex > lastLogIndex { // preceding entries not exist yet, missing entries that are present on the leader
		reply.Success = false
		if OPT_BACK_UP_INCONSITENT_IN_TERM {
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XNextIndex = lastLogIndex+1 // tell leader to sync from here
		}
		return
	}
	if rf.lookupEntryByIndex(args.PrevLogIndex).Term != args.PrevLogTerm { // preceding entry whose term is conflicting, extra entries that are not present on the leader
		reply.Success = false
		if OPT_BACK_UP_INCONSITENT_IN_TERM {
			// find index of the first entry in the conflicting term
			prevEntry := rf.lookupEntryByIndex(args.PrevLogIndex)
			xterm := prevEntry.Term
			xindex := prevEntry.Index
			for xindex > rf.snapshot.lastSnapshotIndex {
				if rf.lookupEntryByIndex(xindex-1).Term != xterm {
					break
				}
				xindex--
			}
			reply.XTerm = xterm // conflicting term
			reply.XIndex = xindex // index of first entry with that term 
			reply.XNextIndex = -1 // useless now...
		}
		return
	}
	reply.Success = true

	nextIndex := args.PrevLogIndex+1
	if nextIndex <= lastLogIndex {
		// (partial) entries already exists in follower's log.
		// Check confliciting entries with leader (If an existing entry conflicts with a new one (same index but different terms)
		if rf.lookupEntryByIndex(nextIndex).Term != args.Term {
			// It conflicts.
			// Discard confliciting entries with leader: delete the existing entry and all that follow it
			rf.discardEntriesFrom(nextIndex)
		} else {
			// (Maybe partial) duplicated AppendEntries from same leader...
			// Check whether some entries can save
			if args.PrevLogIndex+len(args.Entries) > lastLogIndex {
				// check prefix-duplicated entries really identical
				for i:=0; i<lastLogIndex-args.PrevLogIndex; i++ {
					if args.Entries[i].Term != rf.lookupEntryByIndex(args.Entries[i].Index).Term {
						reply.Success = false
						DPrintf("[term%v node%v Follower] faulty leader %v send AppendEntries with same index but different content", rf.currentTerm, rf.me, args.LeaderId)
						return
					}
				}
				args.Entries = args.Entries[lastLogIndex-args.PrevLogIndex:]
			} else {
				// drop the duplicated AppendEntries request
				return
			}
		}
	}
	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)
	rf.persist()

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
	if rf.lastApplied < rf.snapshot.lastSnapshotIndex {
		rf.lastApplied = rf.snapshot.lastSnapshotIndex
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		DPrintf("[node%v applyEntries] apply log %v", rf.me, rf.lastApplied)
		entry := rf.lookupEntryByIndex(rf.lastApplied)
		cmd := entry.Command
		if cmd == nil {
			panic("cmd is nil")
		}
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid:  true,
			Command:       cmd,
			CommandIndex:  rf.lastApplied,
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != ROLE_LEADER {
	index = -1
	term = rf.currentTerm
	isLeader = false
		return
	}
	index = rf.getLastLogIndex() + 1
	term = rf.currentTerm
	isLeader = true
	entry := Entry{
		Term: term,
		Index: index,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	rf.nextIndex[rf.me]= index + 1
	rf.matchIndex[rf.me]= index
	DPrintf("[term%v node%v Leader] append an new entry %v", rf.currentTerm, rf.me, entry)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	DPrintf("node %v stop\n %#v", rf.me, rf)
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
		rf.role = ROLE_CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persist()

		DPrintf("[term%v node%v Candidate] start an election", rf.currentTerm, rf.me)

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
	if !ok {
		DPrintf("[term%v node%v Candidate] request peer %v 's vote failed", args.Term, args.CandidateId, peerIndex)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	rf.me = me
	rf.majarity = len(rf.peers)/2+1

	rf.role = ROLE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.persister = persister
	rf.log = make([]Entry, 0, initialLogQueueCapacity)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	DPrintf("node %v start\n %#v", rf.me, rf)

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
	DPrintf("[term%v node%v Leader Replication] startReplication", term, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != ROLE_LEADER || rf.currentTerm != term {
		if rf.role != ROLE_LEADER {
			DPrintf("[term%v node%v Leader Replication] stop because no longer leader", term, rf.me)
		} else {
			DPrintf("[term%v node%v Leader Replication] stop because new term %v", term, rf.me, rf.currentTerm)
		}
		return false
	}

	installSnapShotToPeer := func(peer int, args *InstallSnapshotArgs) bool {
		reply := &InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if ok {
			// discover newer term
			if reply.Term > rf.currentTerm {
				rf.intoFollower(reply.Term)
				return ok // stop since no longer leader
			}
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		return ok
	}
	appendEntriesToPeer := func(peer int, args *AppendEntriesArgs, lastEntryIndex int) {
		reply := &AppendEntriesReply{}
		DPrintf("[term%v node%v Leader Replication] peer %v sendAppendEntries", args.Term, args.LeaderId, peer)
		ok := rf.sendAppendEntries(peer, args, reply)
		if !ok {
			DPrintf("[term%v node%v Leader Replication] peer %v sendAppendEntries failed", args.Term, args.LeaderId, peer)
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.role != ROLE_LEADER || rf.currentTerm != args.Term {
			DPrintf("[term%v node%v Leader Replication] peer %v stop", term, rf.me, peer)
			return
		}

		// discover newer term
		if reply.Term > rf.currentTerm {
			rf.intoFollower(reply.Term)
			return // stop since no longer leader
		}
		if reply.Success {
			rf.nextIndex[peer] = lastEntryIndex + 1
			rf.matchIndex[peer] = lastEntryIndex
			DPrintf("[term%v node%v Leader Replication] peer %v AppendEntries Success num_entries:%v nextIndex:%v Entries:%#v", term, rf.me, peer, len(args.Entries), rf.nextIndex[peer], args.Entries)
		} else {
			if rf.nextIndex[peer] == 1 {
				// impossible. faulty peer since log 0(empty sentinel) must be consistent
				DPrintf("[term%v node%v Leader Replication] faulty peer %v inconsistent with sentinel log entry.", term, rf.me, peer)
				return // give up to replicate to this peer
			}

			if OPT_BACK_UP_INCONSITENT_IN_TERM {
				if reply.XTerm == -1 {
					// follower's log is too short, reset nextIndex to next index of the follower's log
					rf.nextIndex[peer] = reply.XNextIndex
				} else {
					// follower's log has conflicting entries.
					lastLogIndex := rf.getLastLogIndex()
					if reply.XIndex > lastLogIndex {
						// impossible. faulty peer since reply.XIndex <= args.PrevLogIndex <= lastLogIndex
						DPrintf("[term%v node%v Leader Replication] faulty peer %v reply XIndex > lastLogIndex.", term, rf.me, peer)
						return // give up to replicate to this peer
					}
					// the conflicting term maybe partially valid (commited)
					//
					// if entry exists in leader's log, it must be commited already
					// so, if any entry whose term is XTerm , the XIndex is valid and 
					// otherwise, the entrire term is invalid and need to be overrite
					if reply.XIndex < rf.snapshot.lastSnapshotIndex || rf.lookupEntryByIndex(reply.XIndex).Term == reply.XTerm {
						// XIndex is index of first entry with that term, and maybe more entries are commited alreay in leader's log
						nextIndex := reply.XIndex + 1
						if nextIndex <= rf.snapshot.lastSnapshotIndex {
							nextIndex = rf.snapshot.lastSnapshotIndex + 1
						}
						for nextIndex <= lastLogIndex && rf.lookupEntryByIndex(nextIndex).Term == reply.XTerm {
							nextIndex++
						}
						rf.nextIndex[peer] = nextIndex
					} else {
						// XIndex is index of first entry with that term, so there is no one entry whose term is XTerm in leader's log
						rf.nextIndex[peer] = reply.XIndex
					}
				}
			} else {
				rf.nextIndex[peer]--
			}
			DPrintf("[term%v node%v Leader Replication] peer %v nextIndex updated %v", term, rf.me, peer, rf.nextIndex)
		}
	}

	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}

		if rf.nextIndex[peerIdx] <= rf.snapshot.lastSnapshotIndex {
			data := make([]byte, len(rf.snapshot.data))
			copy(data, rf.snapshot.data) // avoiding data-race, we will unlock during the request
			args := &InstallSnapshotArgs{
				Term:              term,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.snapshot.lastSnapshotIndex,
				LastIncludedTerm:  rf.snapshot.lastSnapshotTerm,
				Data:              data,
			}
			go installSnapShotToPeer(peerIdx, args)
		} else {
			nextIndex := rf.nextIndex[peerIdx]
			lastLogIndex := rf.getLastLogIndex()
			prevLogIndex := nextIndex-1
			prevLogTerm := rf.lookupEntryByIndex(nextIndex-1).Term
			entries := rf.lookupEntriesByIndex(nextIndex, lastLogIndex)
			entriesCopy := make([]Entry, len(entries)) // avoiding data-race, we will unlock during the request
			copy(entriesCopy, entries)
			// sanity check for entries
			DPrintf("[term%v node%v Leader Replication] nextIndex%v lastLogIndex%v", term, rf.me, nextIndex, lastLogIndex)
			DPrintf("[term%v node%v Leader Replication] log%#v", term, rf.me, rf.log)
			DPrintf("[term%v node%v Leader Replication] snapshot%#v", term, rf.me, rf.snapshot)
			if len(entriesCopy) > 0  {
				for i, e := range entriesCopy {
					DPrintf("[term%v node%v Leader Replication] entries[%v]: %v", term, rf.me, i, e)
					if entriesCopy[i].Index != nextIndex+i {
						panic("entry's index is incorrect")
					}
					if e.Command == nil {
						panic("AppendEntries cannot include nil entry")
					}
				}
			}
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entriesCopy,
				LeaderCommit: rf.commitIndex,
			}
			go appendEntriesToPeer(peerIdx, args, lastLogIndex)
		}
	}

	// bump commit index in leader if majarity replicated
	lastLogIndex := rf.getLastLogIndex()
	if rf.commitIndex < rf.snapshot.lastSnapshotIndex {
		rf.commitIndex = rf.snapshot.lastSnapshotIndex
	}
	for nextCommitIndex := rf.commitIndex+1; nextCommitIndex<=lastLogIndex; nextCommitIndex++{
		if rf.lookupEntryByIndex(nextCommitIndex).Term != rf.currentTerm {
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