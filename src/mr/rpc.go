package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Register as a worker
type RegisterWorkerArgs struct {

}
type RegisterWorkerReply struct {
	WorkerId int
}

// Request a new assignment
type RequestAssignmentArgs struct {
	WorkerId int
}
type RequestAssignmentReply struct {
	NumberOfMap int
	NumberOfReduce int

	// types:
	// 1 - map. 
	// 2 - reduce.
	TaskType int
	TaskId int
	MapFilename string
}

// Report a complete of map task
type ReportMapCompletedArgs struct {
	MapTaskId int
}
type ReportMapCompletedReply struct {
}
// Report a complete of reduce task
type ReportReduceCompletedArgs struct {
	ReduceTaskId int
}
type ReportReduceCompletedReply struct {
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
