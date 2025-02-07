package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io"
import "os"
import "encoding/json"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//log.SetOutput(io.Discard)
	workerId, err := RegisterWorker()
	if err != nil {
		panic(err)
	}
	log.Println("Registered as worker ", workerId)
	for {
		log.Println("RequestAssignment")
		reply, err := RequestAssignment(workerId)
		if err != nil {
			panic(err)
		}
		log.Println("reply", reply)
		if reply.TaskType == 1 {
			if reply.TaskId >= 0 {
				WorkerDoMap(reply.MapFilename, reply.TaskId, reply.NumberOfReduce, mapf)
				ReportMapCompleted(reply.TaskId)
			} else {
				time.Sleep(1 * time.Second) // waiting for unfinished map task on other workers
			}
		} else if reply.TaskType == 2 {
			if reply.TaskId >= 0 {
				WorkerDoReduce(reply.TaskId, reply.NumberOfMap, reducef)
				// report
				ReportReduceCompleted(reply.TaskId)
			} else {
				time.Sleep(1 * time.Second) // waiting for unfinished map task on other workers
			}
		} else {
			if reply.TaskType == 0 {
				log.Println("worker shutdown gracefully")
				return
			}
			log.Fatal("unknown reply TaskType")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func RegisterWorker() (int, error) {
	// declare an argument structure.
	args := RegisterWorkerArgs{}

	// declare a reply structure.
	reply := RegisterWorkerReply{}

	// the RegisterWorker() method of struct Coordinator.
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		return reply.WorkerId, nil
	} else {
		return -1, fmt.Errorf("RPC RegisterWorker failed")
	}
}


func RequestAssignment(workerId int) (*RequestAssignmentReply, error) {
	// declare an argument structure.
	args := RequestAssignmentArgs{
		WorkerId: workerId,
	}

	// declare a reply structure.
	reply := RequestAssignmentReply{}

	// the RequestAssignment() method of struct Coordinator.
	ok := call("Coordinator.RequestAssignment", &args, &reply)
	if ok {
		return &reply, nil
	} else {
		return nil, fmt.Errorf("RPC RequestAssignment failed")
	}
}

func ReportMapCompleted(taskid int) error {
	// declare an argument structure.
	args := ReportMapCompletedArgs{
		MapTaskId: taskid,
	}

	// declare a reply structure.
	reply := ReportMapCompletedReply{}

	// the ReportMapCompleted() method of struct Coordinator.
	ok := call("Coordinator.ReportMapCompleted", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("RPC ReportMapCompleted failed")
	}
}
func ReportReduceCompleted(taskid int) error {
	// declare an argument structure.
	args := ReportReduceCompletedArgs{
		ReduceTaskId: taskid,
	}

	// declare a reply structure.
	reply := ReportReduceCompletedReply{}

	// the ReportReduceCompleted() method of struct Coordinator.
	ok := call("Coordinator.ReportReduceCompleted", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("RPC ReportReduceCompleted failed")
	}
}


func WorkerDoMap(filename string, maptaskid int, nor int, 
	mapf func(string, string) []KeyValue,
) {
	log.Println("WorkerDoMap taskid", maptaskid, " filename ", filename)
	r, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		log.Fatal(err)
	}
	content := string(b)

	result := mapf(filename, content)

	// a group of output files which is corresponding to different bucket
	ofiles := make([]*os.File, 0, nor)
	encs := make([]*json.Encoder, 0, nor)
	for i:=0; i<nor; i++ {
		// create temporary file then rename later due to atomic visibility
		ofile, err := os.CreateTemp("", "mr-map-temp")
		if err != nil {
			log.Fatal(err)
		}
		enc := json.NewEncoder(ofile)
		ofiles = append(ofiles, ofile)
		encs = append(encs, enc)
	}
	// grouping
	for _, kv := range result {
		bukcet := ihash(kv.Key) % nor
		err := encs[bukcet].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

	// make final result visible
	for i:=0; i<nor; i++ {
		err := ofiles[i].Close()
		if err != nil {
			log.Fatal(err)
		}
		oname := map_result_file_name(maptaskid, i)
		os.Rename(ofiles[i].Name(), oname) // It is safe to call Name after [Close].
	}
}

func WorkerDoReduce(reducetaskid int, nom int,
	reducef func(string, []string) string,
) {
	log.Println("WorkerDoReduce taskid", reducetaskid)
	key2values := make(map[string][]string)
	key2reduced := make(map[string]string)
	for i:=0; i<nom; i++ {
		filename := map_result_file_name(i, reducetaskid)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
		  var kv KeyValue
		  if err := dec.Decode(&kv); err != nil {
			break
		  }
		  key2values[kv.Key] = append(key2values[kv.Key], kv.Value)
		}
	}
	for key, values := range key2values {
		reduced := reducef(key, values)
		key2reduced[key] = reduced
	}

	// create temporary file then rename later due to atomic visibility
	ofile, err := os.CreateTemp("", "mr-out-reduce-temp")
	if err != nil {
		log.Fatal(err)
	}
	for key, value := range key2reduced {
		fmt.Fprintf(ofile, "%v %v\n", key, value)
	}
	// make final result visible
	oname := reduce_result_file_name(reducetaskid)
	os.Rename(ofile.Name(), oname) // It is safe to call Name after [Close].
}

func map_result_file_name(maptaskid, reducetaskid int) string {
	return fmt.Sprintf("mr-%v-%v", maptaskid, reducetaskid)
}
func reduce_result_file_name(reducetaskid int) string {
	return fmt.Sprintf("mr-out-%v", reducetaskid)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
