package mr

import (
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType int

const (
	UnknownTask TaskType = iota
	MapTask
	ReduceTask
)

type TaskStatus int

const (
	Waiting TaskStatus = iota
	Running
	Done
)

type Coordinator struct {
	waitingTaskQueue Queue
	runningTaskQueue Queue
	doneTaskQueue    Queue

	mapTotal    int
	reduceTotal int
}

type Task struct {
	id           int        // the work number, use file name
	taskType     TaskType   // the task type, map or reduce
	status       TaskStatus // the task state
	mapTempFiles []string   // the map task create the temp output file
	time         time.Time  // the task start time
	reduceTotal  int
}

type Queue struct {
	tasks []Task
	mutex sync.Mutex
}

func (q *Queue) Push(task Task) {
	q.mutex.Lock()
	q.tasks = append(q.tasks, task)
	q.mutex.Unlock()
}

func (q *Queue) Pop() (Task, error) {
	q.mutex.Lock()
	if q.Empty() {
		q.mutex.Unlock()
		return Task{}, errors.New("queue is empty")
	}
	taskSize := q.Size()
	task := q.tasks[taskSize-1]
	q.tasks = q.tasks[:taskSize-1]
	q.mutex.Unlock()
	return task, nil
}

func (q *Queue) Size() int {
	return len(q.tasks)
}

func (q *Queue) Empty() bool {
	return len(q.tasks) == 0
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c := Coordinator{
		mapTotal:    len(files),
		reduceTotal: nReduce,
	}

	// int the waitingQueue

	// Your code here.

	c.server()
	return &c
}
