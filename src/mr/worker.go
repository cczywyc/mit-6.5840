package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// send the RPC to the coordinator for asking the task in a loop
	for {
		task := callTaskAsking()
		switch task.Type {
		case MapTask:
		case ReduceTask:
		case AllTaskDone:
			goto workExit
		default:
			fmt.Println("Unknown task type!")
		}

	workExit:
		break
	}
}

func callTaskAsking() *Reply {
	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.TaskAsking", &args, &reply)
	if !ok {
		fmt.Printf("call coordinator for asking task failed!\n")
		return nil
	}
	return &reply
}

func doMap(task *Reply) error {
	// todo: doMap implementation
	return nil
}

func doReduce(task *Reply) error {
	// todo: doReduce implementation
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
