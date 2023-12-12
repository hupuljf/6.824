package mr

import (
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type worker struct {
	ID      string //worker的id 唯一标识这个worker进程
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	nReduce int
	nMap    int
}

func (w *worker) AskTask() *WorkerApplyReply {
	args := WorkerApplyArgs{
		w.ID,
	}
	reply := WorkerApplyReply{}
	//fmt.Println(args)
	//reply.TaskId = -1 //取不到默认是-1
	success := call("Coordinator.AssignTask", &args, &reply)
	for !success {
		success = call("Coordinator.AssignTask", &args, &reply)
	}
	if reply.TaskId == -1 {
		return nil
	}
	//fmt.Println(reply)
	w.nMap = reply.MapNumber
	w.nReduce = reply.ReduceNumber
	return &reply

}

func (w *worker) ReportState(taskId int, state string) error {
	args := UpdateApplyArgs{
		TaskId:    taskId,
		TaskState: state,
	}
	call("Coordinator.UpdateWorkingState", &args, &UpdateReply{})
	return nil
}
func (w *worker) Run() {
	for {
		time.Sleep(time.Second)
		t := w.AskTask()
		if t == nil {
			log.Println("All task has been finished,worker", w.ID, "closed")
			break
		}
		w.doTask(t)
	}
}

func (w *worker) doTask(t *WorkerApplyReply) {
	fmt.Println("此时的taskdetail：", t.TaskDetail)
	switch t.TaskDetail.Phase {
	case "map":
		log.Println("Worker", w.ID, ":do map task ", t.TaskId)
		w.doMapTask(t)

	case "reduce":
		log.Println("Worker", w.ID, "do reduce task ", t.TaskId)
		w.doReduceTask(t)
	default:
		log.Fatalln("Error in doTask , wrong phase", t.TaskId)
	}
}

func (w *worker) doMapTask(t *WorkerApplyReply) {
	content, err := readFile(t.TaskDetail.FileName) //m个map任务实际上 m个文件
	fmt.Println("doing map", t.TaskDetail.FileName)
	if err != nil {
		log.Fatalln("Failed to read file ", t.TaskDetail.FileName)
		w.ReportState(t.TaskId, "failed")
	}

	kvs := w.mapf(t.TaskDetail.FileName, content)
	//partioning kvs for n reduce task

	// each for a file
	reduces := make([][]KeyValue, w.nReduce)

	for _, kv := range kvs {
		//hash the key and put into correspond reduce
		i := ihash(kv.Key) % w.nReduce
		reduces[i] = append(reduces[i], kv)
	}

	//write into files

	for index, r := range reduces { //每个map任务 根据他里面的key hash到n个reduce
		filename := fmt.Sprintf("./mr-%d-%d.json", t.TaskId, index)
		if saveKV2File(filename, r) != nil {
			log.Fatalln("Failed to save to file ", filename)
			w.ReportState(t.TaskId, "failed")
		}
	}

	w.ReportState(t.TaskId, "complete")
}

//do reduce task
func (w *worker) doReduceTask(t *WorkerApplyReply) {
	m := make(map[string][]string)
	for i := 0; i < w.nMap; i++ {
		filename := fmt.Sprintf("./mr-%d-%d.json", i, t.TaskId)
		if readFile2Maps(filename, m) != nil {
			log.Fatalln("Failed to read file ", filename)
			w.ReportState(t.TaskId, "failed")
		}
	}
	results := make([]string, 0, 100)

	for k, v := range m {
		results = append(results, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if ioutil.WriteFile(fmt.Sprintf("./mr-out-%d", t.TaskId), []byte(strings.Join(results, "")), 0600) != nil {
		log.Fatalln("write reduce file failed")
		w.ReportState(t.TaskId, "failed")
	}
	w.ReportState(t.TaskId, "complete")
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

	workerId, _ := uuid.NewUUID()

	w := worker{
		ID:      workerId.String(),
		mapf:    mapf,
		reducef: reducef,
	}
	fmt.Println(w)
	//CallExample()
	w.Run()

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
	fmt.Println(args)

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)
	//fmt.Println(reply)
	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
