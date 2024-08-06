package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// declare the arguments
// and reply for an RPC.
//

type (
	GetTaskRequest struct {
		// list of file processed
		Done     []string
		WorkerID string
	}
	GetTaskResponse struct {
		FilePaths []string
		NReduce   int
	}
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
