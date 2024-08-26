package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Phase int

const (
	MapPhase Phase = iota + 1
	ReducePhase
	Done
)

type TaskType int

const (
	MapTask    TaskType = iota + 1
	ReduceTask          // reduce task
)

type TaskStatus int

const (
	Waiting TaskStatus = iota + 1
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

	log.Printf("[Info]: Coordinator receive the getTask request, args: %v \n", args)
	if c.baseInfo.workerMap[args.WorkerName] == nil {
		c.baseInfo.workerMap[args.WorkerName] = &WorkInfo{
			name:           args.WorkerName,
			lastOnlineTime: time.Now(),
		}
	} else {
		c.baseInfo.workerMap[args.WorkerName].lastOnlineTime = time.Now()
	}

	switch c.phase {
	case MapPhase:
		task := getTask(c.baseInfo.taskMap[MapTask])
		if task != nil {
			task.WorkName = args.WorkerName
			task.Status = Running
			log.Printf("[Info]: Assign the map task number: %d to worker: %s \n", task.Id, args.WorkerName)
		}
		reply.Task = task
	case ReducePhase:
		task := getTask(c.baseInfo.taskMap[ReduceTask])
		if task != nil {
			task.WorkName = args.WorkerName
			task.Status = Running
			log.Printf("[Info]: Assign the reduce task number: %d to worker: %s \n", task.Id, args.WorkerName)
		}
		reply.Task = task
	case Done:
		// worker exit
		task := &Task{TaskType: 0}
		reply.Task = task
		// close the timer
		close(c.timer)
	default:
		log.Printf("[Error]: Coordinator unknown phase: %v \n", c.phase)
		return errors.New("coordinator unknown phase")
	}

	return nil
}

// taskDoneHandler is the RPC handler for theo workers to finish the task
func (c *Coordinator) taskDoneHandler(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	log.Printf("[Info]: Coordinator receive the task done request, args: %v \n", args)
	c.baseInfo.workerMap[args.WorkerName].lastOnlineTime = time.Now()

	task := args.Task
	switch task.TaskType {
	case MapTask:
		task.Status = Finished
		if !checkTask(c.baseInfo.taskMap[MapTask]) {
			log.Printf("[Info]: All map tasks have benn finished, the reduce phase begins")

			reduceInputMap := make(map[int][]string)
			for _, mapTask := range c.baseInfo.taskMap[MapTask] {
				for _, fileName := range mapTask.Output {
					split := strings.Split(fileName, "-")
					idxReduce, _ := strconv.Atoi(split[2])
					reduceInputMap[idxReduce] = append(reduceInputMap[idxReduce], fileName)
				}
			}
			// init the reduce tasks
			var reduceTasks []*Task
			for i := range c.baseInfo.nReduce {
				task := &Task{
					Id:       i,
					TaskType: ReduceTask,
					Status:   Waiting,
					Input:    reduceInputMap[i],
					Output:   []string{},
				}
				reduceTasks = append(reduceTasks, task)
			}
			c.baseInfo.taskMap[ReduceTask] = reduceTasks

			c.phase = ReducePhase
		}
	case ReduceTask:
		task.Status = Finished
		if !checkTask(c.baseInfo.taskMap[ReduceTask]) {
			log.Printf("[Info]: All reduce tasks have benn finished, the done phase begins")
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

// workerTimer create a timer that checks the worker online status every 1 second
func (c *Coordinator) workerTimer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			select {
			case <-c.timer:
				log.Printf("[Info]: Worker timer exit. \n]")
				return
			default:
				c.mutex.Lock()

				for _, workInfo := range c.baseInfo.workerMap {
					if time.Now().Sub(workInfo.lastOnlineTime) <= 10 {
						continue
					}
					// According to the MapReduce paper, in a real distributed system,
					// since the intermediate files output by the Map task are stored on their respective worker nodes,
					// when the worker is offline and cannot communicate, all map tasks executed by this worker,
					// whether completed or not, should be reset to the initial state and reallocated to other workers,
					// while the files output by the reduce task are stored on the global file system (GFS),
					// and only unfinished tasks need to be reset and reallocated.
					if c.phase == MapPhase {
						mapTasks := c.baseInfo.taskMap[MapTask]
						for _, task := range mapTasks {
							if task.WorkName == workInfo.name {
								task.Status = Waiting
								task.WorkName = ""
								for _, output := range task.Output {
									_ = os.Remove(output)
								}
								task.Output = []string{}
							}
						}
					} else if c.phase == ReducePhase {
						reduceTasks := c.baseInfo.taskMap[ReduceTask]
						for _, task := range reduceTasks {
							if task.WorkName == workInfo.name && task.Status == Running {
								task.Status = Waiting
								task.WorkName = ""
								_ = os.Remove(task.Output[0])
								task.Output = []string{}
							}
						}
					}
					delete(c.baseInfo.workerMap, workInfo.name)
				}
				c.mutex.Unlock()
			}
		}
	}()
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

	// init the map tasks
	for i, file := range files {
		task := &Task{
			Id:       i,
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

	// worker online status timer
	c.workerTimer()
	// start RPC server
	c.server()
	return &c
}
