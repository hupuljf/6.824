package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type WorkerApplyArgs struct {
	WorkerId string
}

type WorkerApplyReply struct {
	TaskId       int //如果为-1 代表任务全部完成 没有对应的任务id
	MapNumber    int
	ReduceNumber int
	TaskDetail   TaskDetail
}

type UpdateApplyArgs struct {
	TaskId    int
	TaskState string
}

type UpdateReply struct {
}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket nameR
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
