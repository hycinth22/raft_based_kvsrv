package raftapi

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// indicate to Raft code that is should cleanup any long-running go routines.
	Kill()
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the server (or
// tester), via the applyCh passed to Make(). Set CommandValid to true
// to indicate that the ApplyMsg contains a newly committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
