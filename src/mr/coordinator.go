package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"
//import "io"
import "sync/atomic"

type Coordinator struct {
	// Your definitions here.
	nMap int
	nReduce int
	files []string


	nextWorkerId atomic.Int64
	stage atomic.Int64 // 0 maping 1 reducing 2 finish

	mapFinished []atomic.Bool
	mapFinishedCnt atomic.Int64
	mapNext chan int

	reduceFinished []atomic.Bool
	reduceFinishedCnt atomic.Int64
	reduceNext chan int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	reply.WorkerId = int(c.nextWorkerId.Add(1))
	log.Println("Register new worker #", reply.WorkerId)
	return nil
}

func (c *Coordinator) RequestAssignment(args *RequestAssignmentArgs, reply *RequestAssignmentReply) error {
	reply.NumberOfMap = c.nMap
	reply.NumberOfReduce = c.nReduce

	worker := args.WorkerId
	for retry := 0; retry < 30; retry++ {
		stage := c.stage.Load()
		if stage == 0 {
			reply.TaskType = 1
			maptaskid, maptaskfile := c.NextUnmappedFile()
			reply.TaskId = maptaskid
			reply.MapFilename = maptaskfile
			if maptaskid >= 0 {
				log.Println("#", worker, " RequestAssignment and assigned MapTask", maptaskid)
				go func() {
					// timeout handler
					time.Sleep(10 * time.Second) 
					if !c.mapFinished[maptaskid].Load() {
						c.mapNext <- maptaskid
					}
				}()
				return nil
			} else {
				log.Println("#", worker, " RequestAssignment but waiting for pending MapTask")
				time.Sleep(100*time.Millisecond)
			}
		} else if stage == 1 {
			reply.TaskType = 2
			reducetaskid := c.NextUncompletedReduce()
			reply.TaskId = reducetaskid
			if reducetaskid >= 0 {
				log.Println("#", worker, " RequestAssignment and assigned ReduceTask", reducetaskid)
				go func() {
					// timeout handler
					time.Sleep(10 * time.Second) 
					if !c.reduceFinished[reducetaskid].Load() {
						c.reduceNext <- reducetaskid
					}
				}()
				return nil
			} else {
				log.Println("#", worker, " RequestAssignment but waiting for pending ReduceTask")
				time.Sleep(100*time.Millisecond)
			}
		} else {
			reply.TaskType = 0 // tell worker to exit gracefully
			return nil
		}
	}
	return nil
}

func (c *Coordinator) ReportMapCompleted(args *ReportMapCompletedArgs, reply *ReportMapCompletedReply) error {
	taskid := args.MapTaskId
	if taskid < 0 || taskid >= c.nMap {
		return fmt.Errorf("illgeal RPC ReportMapCompleted arg")
	}
	if c.mapFinished[taskid].CompareAndSwap(false, true) {
		log.Println("ReportMapCompleted ", taskid)
		if c.mapFinishedCnt.Add(1) == int64(c.nMap) {
			log.Println("all map task finished!!!")

			c.reduceNext = make(chan int, c.nReduce)
			c.reduceFinished = make([]atomic.Bool, c.nReduce)
			c.reduceFinishedCnt.Store(0)
			for i:=0; i<c.nReduce; i++ {
				c.reduceNext <- i
			}
			c.stage.Store(1) // start to reduce
		}
	} else {
		log.Println("duplicated ReportMapCompleted ", taskid)
	}
	return nil
}

func (c *Coordinator) ReportReduceCompleted(args *ReportReduceCompletedArgs, reply *ReportReduceCompletedReply) error {
	taskid := args.ReduceTaskId
	if taskid < 0 || taskid >= c.nReduce {
		return fmt.Errorf("illgeal RPC ReportReduceCompleted arg")
	}
	if c.reduceFinished[taskid].CompareAndSwap(false, true) {
		log.Println("ReportReduceCompleted ", taskid)
		if c.reduceFinishedCnt.Add(1) == int64(c.nReduce) {
			log.Println("all reduce task finished!!!")
			c.stage.Store(2) // start to shutdown
		}
	} else {
		log.Println("duplicated ReportReduceCompleted ", taskid)
	}
	return nil
}



//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.stage.Load() == 2

	return ret
}

func (c *Coordinator) NextUnmappedFile() (int, string) {
	select {
	case id := <- c.mapNext:
		return id, c.files[id]
	default:
		return -1, ""
	}
}
func (c *Coordinator) NextUncompletedReduce() int {
	select {
	case id := <- c.reduceNext:
		return id
	default:
		return -1
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//log.SetOutput(io.Discard)
	nFiles := len(files)
	c.stage.Store(0)
	c.nReduce = nReduce;
	c.nMap = nFiles
	c.files = files
	c.mapNext = make(chan int, nFiles)
	c.mapFinished = make([]atomic.Bool, nFiles)
	c.mapFinishedCnt.Store(0)
	for i:=0; i<nFiles; i++ {
		c.mapNext <- i
	}

	c.server()
	return &c
}
