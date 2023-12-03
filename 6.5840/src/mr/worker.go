package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		getTaskArgs := GetTaskArgs{}
		getTaskReply := GetTaskReply{}
		for retryTime := 0; ; {
			ok := call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)
			if ok {
				break
			}
			retryTime++
			if retryTime == 3 {
				return
			}
			time.Sleep(time.Second)
		}

		//对于需要特殊处理的错误码进行特殊处理
		if getTaskReply.RetCode == RetJobFinish {
			return
		}

		if getTaskReply.RetCode == RetRetryLater {
			time.Sleep(1 * time.Second)
			continue
		}
		//正常的处理流程
		success := false
		switch getTaskReply.Task {
		case MapTask:
			success = MapTaskHandle(mapf, getTaskReply.TaskID, getTaskReply.ReduceNum, getTaskReply.HandleFileName)
		case ReduceTask:
			success = ReduceTaskHandle(reducef, getTaskReply.TaskID, getTaskReply.MapNum)
		}
		finishTaskArgs := FinishTaskArgs{Task: getTaskReply.Task, TaskID: getTaskReply.TaskID, Success: success}
		finishTaskReply := FinishTaskReply{}
		for retryTime := 0; ; {
			ok := call("Coordinator.FinishTask", &finishTaskArgs, &finishTaskReply)
			if ok {
				break
			}
			retryTime++
			if retryTime == 3 {
				return
			}
			time.Sleep(time.Second)
		}
		if finishTaskReply.RetCode == RetJobFinish {
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func MapTaskHandle(pMapf func(string, string) []KeyValue, pMapTaskID int, pReduceNum int, pFileName string) bool {
	handleFile, err := os.Open(pFileName)
	if err != nil {
		log.Fatalf("cannot open %v", pFileName)
		return false
	}
	content, err := ioutil.ReadAll(handleFile)
	if err != nil {
		log.Fatalf("cannot read %v", pFileName)
		return false
	}
	handleFile.Close()
	intermediate := pMapf(pFileName, string(content))

	//先做切分
	reduceBucket := make([][]KeyValue, pReduceNum)
	for i := 0; i < pReduceNum; i++ {
		reduceBucket[i] = make([]KeyValue, 0)
	}
	for _, value := range intermediate {
		bucketNum := ihash(value.Key) % pReduceNum
		reduceBucket[bucketNum] = append(reduceBucket[bucketNum], value)
	}
	//对每个Bucket做排序
	for _, value := range reduceBucket {
		sort.Sort(ByKey(value))
	}
	//创建临时文件，将数据写入，然后重命名临时文件
	for bucketdNum, keyValueS := range reduceBucket {
		tmpFileName := fmt.Sprintf("%v-mr-%v-%v", time.Now().Unix(), pMapTaskID, bucketdNum)
		tmpFile, err := os.Create(tmpFileName)
		if err != nil {
			return false
		}
		for _, oneKeyValue := range keyValueS {
			fmt.Fprintf(tmpFile, "%v %v\n", oneKeyValue.Key, oneKeyValue.Value)
		}
		tmpFile.Close()
		FinalFileName := fmt.Sprintf("mr-%v-%v", pMapTaskID, bucketdNum)
		os.Rename(tmpFileName, FinalFileName)
	}
	return true

}
func ReduceTaskHandle(reducef func(string, []string) string, pReduceTaskID int, pMapTaskNum int) bool {
	intermediate := make([]KeyValue, 0)
	for i := 0; i < pMapTaskNum; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, pReduceTaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			oneKeyValue := KeyValue{}
			fmt.Sscanf(scanner.Text(), "%v %v", &oneKeyValue.Key, &oneKeyValue.Value)
			intermediate = append(intermediate, oneKeyValue)
		}
	}
	sort.Sort(ByKey(intermediate))
	tmpOutputFileName := fmt.Sprintf("%v-mr-out-%v", time.Now().Unix(), pReduceTaskID)
	outputFile, _ := os.Create(tmpOutputFileName)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	outputFile.Close()
	outputFileName := fmt.Sprintf("mr-out-%v", pReduceTaskID)
	os.Rename(tmpOutputFileName, outputFileName)
	return true
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
