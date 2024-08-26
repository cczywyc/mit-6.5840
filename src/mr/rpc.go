package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

// GetTaskArgs is the rpc request when the workers ask for task from master
type GetTaskArgs struct {
	WorkerName string // the worker name
}

// GetTaskReply is the rpc response when the workers ask for task from master
type GetTaskReply struct {
	Task *Task
}

// TaskDoneArgs is the rpc request when the workers finish the task
type TaskDoneArgs struct {
	WorkerName string
	Task       *Task
}

// TaskDoneReply is the rpc response when the workers finish the task
type TaskDoneReply struct {
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
