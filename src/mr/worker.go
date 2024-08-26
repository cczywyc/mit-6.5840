package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"strconv"
	"time"
)

type WorkerS struct {
	name    string
	mapF    func(string, string) []KeyValue
	reduceF func(string, []string) string
}

// KeyValue is the key/value pire of the map functions
type KeyValue struct {
	Key   string
	Value string
}

// ihash uses the hash algorithm to assign the same key to the same reduce task,
// these same keys are written to a temporary file with the same reduce number.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by main/mrworker.go
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerS{
		name:    "worker_" + strconv.Itoa(rand.Intn(100000)),
		mapF:    mapf,
		reduceF: reducef,
	}

	// send the RPC to the coordinator for asking the task in a loop
	for {
		reply := callGetTask(w.name)
		if reply.Task == nil {
			// can not get the task, maybe all map tasks or all reduce task are running but not be finished
			// waiting to the next phase
			time.Sleep(time.Second)
		}

		fmt.Printf("[Info]: Worker: Receive the task: %v \n", reply)
		var err error
		switch reply.Task.TaskType {
		case MapTask:
			err = w.doReduce(reply.Task)
		case ReduceTask:
			err = w.doMap(reply.Task)
		default:
			// worker exit
			fmt.Printf("[Info]: Worker name: %s exit.\n", w.name)
			return
		}
		if err == nil {
			callTaskDone(reply.Task)
		}
	}
}

// callGetTask send RPC request to coordinator for asking task
func callGetTask(workName string) *GetTaskReply {
	args := GetTaskArgs{
		WorkerName: workName,
	}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("[Error]: Coordinator.GetTask failed!\n")
		return nil
	}
	return &reply
}

func callTaskDone(task *Task) {
	task.Status = Finished
	args := TaskDoneArgs{
		Task: task,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		fmt.Printf("[Error]: Coordinator.TaskDone failed!\n")
	}
}

func (w *WorkerS) doMap(reply *Task) error {
	return nil
}

func (w *WorkerS) doReduce(reply *Task) error {
	return nil
}

// call send an RPC request to the coordinator, wait for the response.
// usually returns true, returns false if something goes wrong.
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
