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

// Args is the rpc request argument
type Args struct {
}

// Reply is the rpc request response
type Reply struct {
	ID      int // the map or reduce task id
	Type    TaskType
	File    string // the input file
	NReduce int    // the total number of reduce tasks
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
