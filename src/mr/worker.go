package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
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

		log.Printf("[Info]: Worker: Receive the task: %v \n", reply)
		var err error
		switch reply.Task.TaskType {
		case MapTask:
			err = w.doMap(reply)
		case ReduceTask:
			err = w.doReduce(reply)
		default:
			// worker exit
			log.Printf("[Info]: Worker name: %s exit.\n", w.name)
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
		log.Printf("[Error]: Coordinator.GetTask failed!\n")
		return nil
	}
	return &reply
}

func callTaskDone(task *Task) {
	args := TaskDoneArgs{
		Task: task,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		log.Printf("[Error]: Coordinator.TaskDone failed!\n")
	}
}

// doMap execute the map task
func (w *WorkerS) doMap(reply *GetTaskReply) error {
	task := reply.Task
	if len(task.Input) == 0 {
		log.Printf("[Error]: task number %d: No input!\n", task.Id)
		return errors.New("map task no input")
	}
	log.Printf("[Info]: Worker name: %s start execute number: %d map task \n", w.name, task.Id)

	fileName := task.Input[0]
	inputBytes, err := os.ReadFile(fileName)
	if err != nil {
		log.Printf("[Error]: read map task input file error: %v \n", err)
		return err
	}

	// kv2ReduceMap: key: reduce index, value: key/value list. split the same key into reduce
	kv2ReduceMap := make(map[int][]KeyValue, reply.NReduce)
	var output []string
	outputFileNameFunc := func(idxReduce int) string {
		return fmt.Sprintf("mr-%d-%d", task.Id, idxReduce)
	}

	// call the map function
	mapResult := w.mapF(fileName, string(inputBytes))
	for _, kv := range mapResult {
		idxReduce := ihash(kv.Key) % reply.NReduce
		kv2ReduceMap[idxReduce] = append(kv2ReduceMap[idxReduce], kv)
	}

	for idxReduce, kvs := range kv2ReduceMap {
		outputFileName := outputFileNameFunc(idxReduce)
		outputFile, _ := os.Create(outputFileName)
		encoder := json.NewEncoder(outputFile)
		for _, kv := range kvs {
			err := encoder.Encode(kv)
			if err != nil {
				log.Printf("[Error]: write map task output file error: %v \n", err)
				_ = outputFile.Close()
				break
			}
		}
		_ = outputFile.Close()
		output = append(output, outputFileName)
	}

	task.Output = output
	log.Printf("[Info]: Worker name: %s finished the map task number: %d \n", w.name, task.Id)
	return nil
}

// doReduce execute the reduce task
func (w *WorkerS) doReduce(reply *GetTaskReply) error {
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

	log.Println(err)
	return false
}
