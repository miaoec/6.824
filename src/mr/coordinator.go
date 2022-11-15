package mr

import "C"
import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

//type TaskStatus int
//
//const (
//	//TaskStatus Ready
//	TaskStatusDone = iota
//	TaskStatusRunning
//	TaskStatusReady
//)
//
//type Task struct {
//	File   string
//	Offset int
//	Status TaskStatus
//}

type ReduceData struct {
	Key   string
	Value []string
}

type Coordinator struct {
	LogFile io.Writer
	Files   []KeyValue
	nReduce int
	//MappedData    []KeyValue
	Mapped     bool
	Reduced    bool
	MapTask    []Task
	ReduceTask []Task
	reduceSize int
	sync.Mutex
}

type WorkerStatusType int32

const (
	WorkerStatusTypeRunning WorkerStatusType = iota
	WorkerStatusWaiting
	WorkerStatusDone
)

type WorkerStatus struct {
	Offset int
	Resp
	Status       WorkerStatusType
	lastCallTime time.Time
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func (c *Coordinator) checkoutTimeout() {
	for {
		c.Lock()
		if !c.Mapped {
			for i, task := range c.MapTask {
				if task.Status == WorkerStatusTypeRunning {
					if time.Since(task.Timestamp) > 10*time.Second {
						c.MapTask[i].Status = WorkerStatusWaiting
						c.log("mapTask %+v is timeout in %s ", task, time.Now().String())
					}
				}
			}
		} else if !c.Reduced {
			for i, task := range c.ReduceTask {
				if task.Status == WorkerStatusTypeRunning {
					if time.Since(task.Timestamp) > 10*time.Second {
						c.ReduceTask[i].Status = WorkerStatusWaiting
						c.log("reduceTask %+v is timeout in %s ", task, time.Now().String())
					}
				}
			}
		}
		c.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (c *Coordinator) reduceTask(reply *Resp) {
	for i, task := range c.ReduceTask {
		if task.Status == WorkerStatusWaiting {
			c.ReduceTask[i].Status = WorkerStatusTypeRunning
			c.ReduceTask[i].Timestamp = time.Now()
			reply.Task = c.ReduceTask[i]
			return
		}
	}
	//if len(c.MappedData) != 0 {
	//	key, value := c.popData(c.reduceSize)
	//	reply.ReduceData = ReduceData{Key: key, Value: value}
	//	reply.Type = TaskTypeReduce
	//	reply.Offset = len(c.WorkStatus)
	//	c.WorkStatus = append(
	//		c.WorkStatus, &WorkerStatus{
	//			Resp:         *reply,
	//			Status:       WorkerStatusTypeRunning,
	//			lastCallTime: time.Now(),
	//		},
	//	)
	//} else {
	//	reply.Type = TaskTypeNULL
	//}
	return
}

func (c *Coordinator) mapTask(reply *Resp) {
	//reply.Task.Type =
	for i, task := range c.MapTask {
		if task.Status == WorkerStatusWaiting {
			c.MapTask[i].Status = WorkerStatusTypeRunning
			c.MapTask[i].Timestamp = time.Now()
			reply.Task = c.MapTask[i]
			return
		}
	}
	//if len(c.Files) > 0 {
	//	reply.MapData = c.Files[0]
	//	c.Files = c.Files[1:]
	//	reply.Type = TaskTypeMap
	//	reply.Offset = len(c.WorkStatus)
	//	c.WorkStatus = append(
	//		c.WorkStatus, &WorkerStatus{
	//			Resp:         *reply,
	//			Status:       WorkerStatusTypeRunning,
	//			lastCallTime: time.Now(),
	//		},
	//	)
	//} else {
	//	reply.Type = TaskTypeNULL
	//}
}

func (c *Coordinator) toReduce() {
	c.Mapped = true
	c.ReduceTask = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.ReduceTask[i] = Task{
			Type:  TaskTypeReduce,
			Index: i, Status: WorkerStatusWaiting,
			ReduceFiles: []string{},
		}
	}
	for _, task := range c.MapTask {
		for _, file := range task.ReduceFiles {
			st := strings.Split(file, "-")
			idx, err := strconv.Atoi(st[len(st)-1])
			if err != nil {
				log.Fatal(err)
			}
			c.ReduceTask[idx].ReduceFiles = append(c.ReduceTask[idx].ReduceFiles, file)
		}
	}

	c.log("finished map,the reduece file is:")
	by, _ := json.Marshal(c.ReduceTask)
	c.log(string(by))
}

func (c *Coordinator) checkMapped() bool {
	if !c.Mapped {
		for _, task := range c.MapTask {
			if task.Status != WorkerStatusDone {
				return false
			}
		}
		c.toReduce()
	}
	return c.Mapped
}

func (c *Coordinator) checkReduced() bool {
	if !c.Reduced {
		for _, task := range c.ReduceTask {
			if task.Status != WorkerStatusDone {
				return false
			}
		}
		c.Reduced = true
	}
	return c.Mapped
}

//	if !c.Mapped {
//		return false
//	}
//	if !c.Reduced {
//		hasRunning := false
//		for offset, status := range c.WorkStatus {
//			if status.Status == WorkerStatusTypeRunning {
//				hasRunning = true
//				if time.Since(status.lastCallTime) > time.Second*5 {
//					log.Printf("WorkerStatusWaiting offset=%+v", offset)
//					switch status.Type {
//					case TaskTypeMap:
//						c.Files = append(c.Files, status.MapData)
//					case TaskTypeReduce:
//						c.pushData(status.ReduceData.Key, status.ReduceData.Value...)
//					}
//					c.WorkStatus[offset].Status = WorkerStatusWaiting
//				}
//			}
//		}
//		if hasRunning {
//			return false
//		}
//		for _, s := range c.MappedData {
//			if len(s) > 1 {
//				return false
//			}
//		}
//		c.Reduced = true
//
//	}
//	return c.Reduced
//}

func (c *Coordinator) Task(args *Req, reply *Resp) error {
	c.Lock()
	defer c.Unlock()
	by, _ := json.Marshal(*args)
	c.log("rececv:%s", string(by))
	if args.Task.Index != -1 { //&& c.[args.Task.Index].Status != WorkerStatusWaiting
		switch args.Task.Type {
		case TaskTypeReduce:
			if c.ReduceTask[args.Task.Index].Status == WorkerStatusTypeRunning {
				c.ReduceTask[args.Task.Index] = args.Task
			}
		case TaskTypeMap:
			if c.MapTask[args.Task.Index].Status == WorkerStatusTypeRunning {
				for _, file := range args.Task.ReduceFiles {
					err := os.Rename("tmp/"+file, file)
					if err != nil {
						log.Fatal(err)
					}
				}
				c.MapTask[args.Task.Index] = args.Task
			}

			//c.MapTask[args.]
		}
	}
	reply.NReduce = c.nReduce
	if !c.checkMapped() {
		c.mapTask(reply)
	} else if !c.checkReduced() {
		c.reduceTask(reply)
	} else {
		go func() {
			time.Sleep(3 * time.Second)
			os.Exit(0)
		}()
		reply.Task.Type = TaskShutdown
	}
	by, _ = json.Marshal(reply)
	c.log("reply:%s", string(by))
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	go c.checkoutTimeout()
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()
	return c.Reduced
}

func (c *Coordinator) log(str string, args ...interface{}) {
	fmt.Fprintf(c.LogFile, str+"\n", args...)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	for _, filename := range files {
		c.MapTask = append(
			c.MapTask, Task{MapFile: filename, Status: WorkerStatusWaiting, Index: len(c.MapTask), Type: TaskTypeMap},
		)
	}
	c.nReduce = nReduce
	f, err := os.OpenFile("mr-logfile.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	c.LogFile = f
	// Your code here.

	c.server()
	return &c
}

//split=>map=>reduce
