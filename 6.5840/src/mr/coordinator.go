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

type JobStage int

const (
	MapStage JobStage = iota
	ReduceStage
	FinishStage
)

// 1.阶段信息 Map阶段， Reduce阶段
// 2.MapTaskID -> 文件的列表
// 3.TaskID -> 上次分配时间  这里不记录分配给哪个worker
type Coordinator struct {
	// Your definitions here.
	jobStage              JobStage
	taskID2FileName       map[int]string
	taskID2LastAssignTime map[int]int64
	mu                    sync.Mutex

	reduceNum int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
*/

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.jobStage == FinishStage {
		reply.RetCode = RetJobFinish
		return nil
	}
	//获取当前时间
	nowUnix := time.Now().Unix()
	for key, value := range c.taskID2LastAssignTime {
		if nowUnix-value > 10 {
			reply.RetCode = RetOK
			reply.TaskID = key
			reply.MapNum = len(c.taskID2FileName)
			reply.ReduceNum = c.reduceNum
			switch c.jobStage {
			case MapStage:
				reply.HandleFileName = c.taskID2FileName[key]
				reply.Task = MapTask
			case ReduceStage:
				reply.Task = ReduceTask
			}
			c.taskID2LastAssignTime[key] = nowUnix
			return nil
		}
	}
	reply.RetCode = RetRetryLater
	return nil
}
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.jobStage == FinishStage {
		reply.RetCode = RetJobFinish
		return nil
	}
	if (c.jobStage == MapStage && args.Task == ReduceTask) || (c.jobStage == ReduceStage && args.Task == MapTask) || !args.Success {
		reply.RetCode = RetOK
		return nil
	}
	delete(c.taskID2LastAssignTime, args.TaskID)
	if len(c.taskID2LastAssignTime) == 0 {
		switch c.jobStage {
		case MapStage:
			c.jobStage = ReduceStage
			for i := 0; i < c.reduceNum; i++ {
				c.taskID2LastAssignTime[i] = 0
			}
		default:
			c.jobStage = FinishStage
			reply.RetCode = RetJobFinish
			return nil
		}
	}
	reply.RetCode = RetOK
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.mu.Lock()
	c.mu.Unlock()
	if c.jobStage == FinishStage {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func (c *Coordinator) Init(files []string, nReduce int) {
	c.jobStage = MapStage
	c.taskID2LastAssignTime = make(map[int]int64)
	c.taskID2FileName = make(map[int]string)
	for key, value := range files {
		c.taskID2FileName[key] = value
		c.taskID2LastAssignTime[key] = 0
	}
	c.reduceNum = nReduce
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.Init(files, nReduce)
	c.server()
	return &c
}
