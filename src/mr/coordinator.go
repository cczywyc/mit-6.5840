package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Done
)

type TaskType int

const (
	MapTask    TaskType = iota
	ReduceTask          // reduce task
)

type TaskStatus int

const (
	Waiting TaskStatus = iota
	Running
	Finished
)

type Task struct {
	Id       int        // the map task or reduce task id
	WorkId   string     // the worker name
	TaskType TaskType   // the task type, map or reduce
	Status   TaskStatus // the task state
	Input    []string   // task input files
	Output   []string   // task output files
	Time     time.Time  // the task startTime
}

type baseInfo struct {
	nReduce     int // the total number of reduce tasks
	mapTasks    []*Task
	reduceTasks []*Task
	workers     map[string]*workInfo
}

type workInfo struct {
	id             string
	lastOnlineTime time.Time
}

type Coordinator struct {
	phase    *Phase
	baseInfo *baseInfo

	mutex sync.Mutex
}

// getTaskHandler is the RPC handler for the workers to get tasks
func (c *Coordinator) getTaskHandler(args *GetTaskArgs, reply *GetTaskReply) error {
	return nil
}

// taskDoneHandler is the RPC handler for theo workers to finish the task
func (c *Coordinator) taskDoneHandler(args *TaskDoneArgs, reply *TaskDoneReply) error {
	return nil
}

// getTask gets the unfinished task
func getTask(mapTasks []*Task) (*Task, error) {
	return nil, nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.server()
	return &c
}
