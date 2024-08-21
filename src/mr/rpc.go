package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Args is the rpc request argument
type Args struct {
}

// Reply is the rpc request response
type Reply struct {
	ID      int    // the work id
	File    string // the input file
	NReduce int    // for map task, need all the reduce numbers
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
