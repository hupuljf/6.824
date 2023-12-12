package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

	ReduceNumber int
	MapNumber    int
	Undo         chan UndoQueue
	Tasks        []TaskDetail
	AllDone      bool
	Phase        string //map or reduce
	Mu           sync.Mutex
}

type UndoQueue struct {
	taskID int //索引任务详情
	//phase string //任务阶段 map/reduce

}

type TaskDetail struct { //任务详情
	Phase     string    //任务阶段 map/reduce
	FileName  string    //对于map任务来说就是给定的*.txt
	State     string    //	任务状态 在 undo/doing/complete/timeout/fail 一开始是undo状态放在undoqueue当中，worker从undoqueue中拿任务时 切换状态为doing 如果所有的state都是complete的时候 重新初始化task数组
	WorkerID  string    // 默认为"no assigned" 表示未被任何worker占据
	StartTime time.Time //任务开始的时间 被worker拿到的时间
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println(reply)
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *WorkerApplyArgs, reply *WorkerApplyReply) error {

	if c.AllDone {
		reply.TaskId = -1
		return nil
	}
	//fmt.Println("调用成功", args)
	task := <-c.Undo //无任务时阻塞 等待分配任务

	c.Tasks[task.taskID].WorkerID = args.WorkerId
	c.Tasks[task.taskID].State = "doing" //被worker拿到了 就切换为doing状态
	c.Tasks[task.taskID].StartTime = time.Now()
	reply.TaskId = task.taskID
	reply.MapNumber = c.MapNumber
	reply.ReduceNumber = c.ReduceNumber
	reply.TaskDetail = c.Tasks[task.taskID]
	return nil

}

//TaskDetail的state 通过rpc调用由worker向coor发送
func (c *Coordinator) UpdateWorkingState(args *UpdateApplyArgs, reply *UpdateReply) error {
	c.Tasks[args.TaskId].State = args.TaskState
	return nil
}

func (c *Coordinator) scheduling() {
	for !c.AllDone {
		go c.schedule()
		time.Sleep(SCHEDULE_INTERVAL)
	}
}

//每次调度都得加锁
func (c *Coordinator) schedule() {
	//	fmt.Println("scheduling...")
	c.Mu.Lock() //不开协程 可以不加锁 图方便只要是有临界区资源访问的地方都加锁了
	defer c.Mu.Unlock()
	allComplete := true
	// fmt.Println(len(m.taskQueue))
	for idx, context := range c.Tasks { //遍历任务context 空闲 准备好 正在运行
		switch context.State {
		case "undo":
			allComplete = false
		case "doing":
			allComplete = false

			if c.timeout(context) {
				fmt.Println("TimeOut----", context)
				c.addTask(idx)
			}
			
		case "complete":
		case "failed":
			fmt.Println("Failed----", context)
			c.addTask(idx)
		default:
			log.Fatalln("error schedule")
		}
	}

	if allComplete {
		if c.Phase == "map" { //全完成了 切换为reduce模式
			c.Phase = "reduce"
			c.Undo = make(chan UndoQueue, c.ReduceNumber) //通道更新 之前的已经清零
			c.Tasks = make([]TaskDetail, 0)
			for i := 0; i < c.ReduceNumber; i++ {
				c.Tasks = append(c.Tasks, TaskDetail{
					Phase: "reduce",
					//FileName: file, //reduce任务无需文件信息
					State:    "undo",
					WorkerID: "no assigned",
					//startTime: time.Time{},
				})
				taskId := i
				c.Undo <- UndoQueue{
					taskID: taskId,
				}

			}
		} else {
			c.AllDone = true
		}
	}

}

func (c *Coordinator) timeout(detail TaskDetail) bool {
	curTime := time.Now()
	interval := curTime.Sub(detail.StartTime)
	//fmt.Println(interval, "  ", MAX_PROCESSING_TIME)
	if interval > 5*time.Second {
		return true
	}
	return false
}

func (c *Coordinator) addTask(taskID int) {
	c.Undo <- UndoQueue{
		taskID: taskID,
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

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
	ret := c.AllDone

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.ReduceNumber = nReduce
	c.MapNumber = len(files)
	c.Tasks = make([]TaskDetail, 0)
	fmt.Println(files, nReduce)
	c.Undo = make(chan UndoQueue, len(files))
	for index, file := range files {
		c.Tasks = append(c.Tasks, TaskDetail{
			Phase:    "map",
			FileName: file,
			State:    "undo",
			WorkerID: "no assigned",
			//startTime: time.Time{},
		})
		taskId := index
		c.Undo <- UndoQueue{
			taskID: taskId,
		}

	}
	c.Phase = "map"
	//fmt.Println(c)
	c.server()
	go c.scheduling()
	return &c
}
