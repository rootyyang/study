package mr

import (
	"fmt"
	"testing"
	"time"
)

func TestCoordinator(t *testing.T) {
	c := Coordinator{}
	c.Init([]string{"file0", "file1", "file2"}, 5)
	if c.jobStage != MapStage || c.reduceNum != 5 || len(c.taskID2FileName) != 3 || len(c.taskID2LastAssignTime) != 3 {
		t.Fatalf("c[%v] error", c)
	}
	getTaskArgs := GetTaskArgs{}
	taskId2FileName := make(map[int]string, 0)
	for i := 0; i < 3; i++ {
		getTaskReply := GetTaskReply{}
		c.GetTask(&getTaskArgs, &getTaskReply)
		if getTaskReply.RetCode != RetOK || getTaskReply.Task != MapTask || getTaskReply.MapNum != 3 || getTaskReply.ReduceNum != 5 {
			t.Fatalf("getTaskReply[%v] error ", getTaskReply)
		}
		taskId2FileName[getTaskReply.TaskID] = getTaskReply.HandleFileName
	}
	if len(taskId2FileName) != 3 {
		t.Fatalf("taskId2FileName[%v] error ", taskId2FileName)
	}
	for key, value := range taskId2FileName {
		if value != fmt.Sprintf("file%v", key) {
			t.Fatalf("TaskId[%v] FileName[%v] error ", key, value)
		}
	}

	getTaskReply := GetTaskReply{}
	c.GetTask(&getTaskArgs, &getTaskReply)
	if getTaskReply.RetCode != RetRetryLater {
		t.Fatalf("getTaskReply=[%v] want = RetRetryLater ", getTaskReply)
	}

	nowUnix := time.Now().Unix()
	c.taskID2LastAssignTime[2] = nowUnix - 11
	getTaskReply = GetTaskReply{}
	c.GetTask(&getTaskArgs, &getTaskReply)
	if getTaskReply.RetCode != RetOK || getTaskReply.HandleFileName != "file2" || getTaskReply.Task != MapTask || getTaskReply.MapNum != 3 || getTaskReply.TaskID != 2 {
		t.Fatalf("getTaskReply[%+v] error ", getTaskReply)
	}

	//测试一下reduceTask完成，是否直接返回ok
	finishTaskArgs := FinishTaskArgs{Task: ReduceTask, TaskID: 4, Success: true}
	finishTaskReply := FinishTaskReply{}
	c.FinishTask(&finishTaskArgs, &finishTaskReply)
	if finishTaskReply.RetCode != RetOK {
		t.Fatalf("FinishTaskReply[%+v] error ", finishTaskReply)
	}
	if len(c.taskID2LastAssignTime) != 3 {
		t.Fatalf("c.taskID2LastAssignTime[%+v] error ", c.taskID2LastAssignTime)
	}

	for i := 0; i < 3; i++ {
		finishTaskArgs := FinishTaskArgs{Task: MapTask, TaskID: i, Success: true}
		finishTaskReply := FinishTaskReply{}
		c.FinishTask(&finishTaskArgs, &finishTaskReply)
		if finishTaskReply.RetCode != RetOK {
			t.Fatalf("FinishTaskReply[%+v] error ", finishTaskReply)
		}
		//调用两次，已经删除的任务，应该是直接返回成功
		c.FinishTask(&finishTaskArgs, &finishTaskReply)
		if finishTaskReply.RetCode != RetOK {
			t.Fatalf("FinishTaskReply[%+v] error ", finishTaskReply)
		}
	}
	if c.jobStage != ReduceStage {
		t.Fatalf("c.jobStage[%+v] error ", c.jobStage)
	}

	taskIDMap := make(map[int]bool, 5)
	for i := 0; i < 5; i++ {
		getTaskReply := GetTaskReply{}
		c.GetTask(&getTaskArgs, &getTaskReply)
		if getTaskReply.RetCode != RetOK || getTaskReply.Task != ReduceTask || getTaskReply.MapNum != 3 || getTaskReply.ReduceNum != 5 {
			t.Fatalf("getTaskReply[%v] error ", getTaskReply)
		}
		taskIDMap[getTaskReply.TaskID] = true
	}
	if len(taskIDMap) != 5 {
		t.Fatalf("taskIDMap[%v] error ", taskIDMap)
	}
	getTaskReply = GetTaskReply{}
	c.GetTask(&getTaskArgs, &getTaskReply)
	if getTaskReply.RetCode != RetRetryLater {
		t.Fatalf("getTaskReply=[%v] want = RetRetryLater ", getTaskReply)
	}
	nowUnix = time.Now().Unix()
	c.taskID2LastAssignTime[4] = nowUnix - 11
	c.GetTask(&getTaskArgs, &getTaskReply)
	if getTaskReply.RetCode != RetOK || getTaskReply.Task != ReduceTask || getTaskReply.MapNum != 3 || getTaskReply.TaskID != 4 {
		t.Fatalf("getTaskReply[%+v] error ", getTaskReply)
	}

	for i := 0; i < 4; i++ {
		finishTaskArgs := FinishTaskArgs{Task: ReduceTask, TaskID: i, Success: true}
		finishTaskReply := FinishTaskReply{}
		c.FinishTask(&finishTaskArgs, &finishTaskReply)
		if finishTaskReply.RetCode != RetOK {
			t.Fatalf("FinishTaskReply[%+v] error ", finishTaskReply)
		}
		//调用两次，已经删除的任务，应该是直接返回成功
		c.FinishTask(&finishTaskArgs, &finishTaskReply)
		if finishTaskReply.RetCode != RetOK {
			t.Fatalf("FinishTaskReply[%+v] error ", finishTaskReply)
		}
	}

	finishTaskArgs = FinishTaskArgs{Task: ReduceTask, TaskID: 4, Success: true}
	finishTaskReply = FinishTaskReply{}
	c.FinishTask(&finishTaskArgs, &finishTaskReply)
	if finishTaskReply.RetCode != RetJobFinish {
		t.Fatalf("FinishTaskReply[%+v] error ", finishTaskReply)
	}
	if c.jobStage != FinishStage {
		t.Fatalf("c.jobStage[%+v] error ", c.jobStage)
	}
}
