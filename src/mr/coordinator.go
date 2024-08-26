package mr

import (
	"fmt"
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
	WorkName string     // the worker name
	TaskType TaskType   // the task type, map or reduce
	Status   TaskStatus // the task state
	Input    []string   // task input files, map task is only one input file
	Output   []string   // task output files, reduce task is only one output file
	Time     time.Time  // the task start time
}

type BaseInfo struct {
	nReduce   int // the total number of reduce tasks
	taskMap   map[TaskType][]*Task
	workerMap map[string]*WorkInfo
}

type WorkInfo struct {
	name           string
	lastOnlineTime time.Time
}

type Coordinator struct {
	phase    Phase
	baseInfo *BaseInfo
	timer    chan struct{}
	mutex    sync.Mutex
}

// getTaskHandler is the RPC handler for the workers to get tasks
func (c *Coordinator) getTaskHandler(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	fmt.Printf("[Info]: Coordinator receive the getTask request, args: %v \n", args)
	if c.baseInfo.workerMap[args.WorkerName] == nil {
		c.baseInfo.workerMap[args.WorkerName] = &WorkInfo{
			name:           args.WorkerName,
			lastOnlineTime: time.Now(),
		}
	} else {
		c.baseInfo.workerMap[args.WorkerName].lastOnlineTime = time.Now()
	}

	var task *Task
	switch c.phase {
	case MapPhase:
		task = getTask(c.baseInfo.taskMap[MapTask])
	case ReducePhase:
		task = getTask(c.baseInfo.taskMap[ReduceTask])
	case Done:
		// worker exit
		task = &Task{TaskType: 2}
		// close the timer
		close(c.timer)
	default:
		fmt.Printf("[Error]: Coordinator unknown phase: %v \n", c.phase)
	}

	// build the reply
	if task != nil {
		task.WorkName = args.WorkerName
		task.Time = time.Now()
	}
	reply.Task = task
	return nil
}

// taskDoneHandler is the RPC handler for theo workers to finish the task
func (c *Coordinator) taskDoneHandler(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	fmt.Printf("[Info]: Coordinator receive the task done request, args: %v \n", args)
	c.baseInfo.workerMap[args.WorkerName].lastOnlineTime = time.Now()

	task := args.Task
	switch task.TaskType {
	case MapTask:
		task.Status = Finished
		if !checkTask(c.baseInfo.taskMap[MapTask]) {
			fmt.Printf("[Info]: All map tasks have benn finished, the reduce phase begins")
			c.phase = ReducePhase
		}
	case ReduceTask:
		task.Status = Finished
		if !checkTask(c.baseInfo.taskMap[ReduceTask]) {
			fmt.Printf("[Info]: All reduce tasks have benn finished, the done phase begins")
			c.phase = Done
		}
	}

	return nil
}

// getTask gets the available task, include the wait tasks or failed tasks
func getTask(tasks []*Task) *Task {
	for _, task := range tasks {
		if task.Status == Waiting {
			return task
		}
	}
	return nil
}

// checkTask check if there are any unfinished tasks. False indicates that all tasks have benn finished
func checkTask(tasks []*Task) bool {
	for _, task := range tasks {
		if task.Status != Finished {
			return true
		}
	}
	return false
}

// server start a thread that listens for RPCs from worker.go
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

// Done return true if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.phase == Done {
		// wait for 10s and return after both the worker and the timer exit
		time.Sleep(10 * time.Second)
		ret = true
	}
	return ret
}

// MakeCoordinator return coordinator, init the map tasks
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var mapTasks []*Task
	for i, file := range files {
		task := &Task{
			Id:       i + 1,
			TaskType: MapTask,
			Status:   Waiting,
			Input:    []string{file},
			Output:   []string{},
		}
		mapTasks = append(mapTasks, task)
	}

	taskMap := make(map[TaskType][]*Task)
	taskMap[MapTask] = mapTasks
	baseInfo := &BaseInfo{
		nReduce:   nReduce,
		taskMap:   taskMap,
		workerMap: make(map[string]*WorkInfo),
	}
	c := Coordinator{
		phase:    MapPhase,
		baseInfo: baseInfo,
		timer:    make(chan struct{}),
	}

	c.server()
	return &c
}
