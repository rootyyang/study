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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RetCodeType int

const (
	RetOK RetCodeType = iota
	RetJobFinish
	RetRetryLater
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetTaskArgs struct {
}

type GetTaskReply struct {
	RetCode RetCodeType
	Task    TaskType

	TaskID         int    //Map和Reduce任务复用该ID
	HandleFileName string //要处理的文件名

	ReduceNum int //Reduce任务的数量
	MapNum    int //Map任务的数量
}

type FinishTaskArgs struct {
	//WorkerID string
	Task    TaskType
	TaskID  int
	Success bool
}
type FinishTaskReply struct {
	RetCode RetCodeType
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
