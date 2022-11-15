package mr

import "C"
import (
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

type ReduceData struct {
	Key   string
	Value []string
}

type Coordinator struct {
	LogFile    io.Writer
	Files      []KeyValue
	nReduce    int
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
						c.log("mapTask %+v is timeout in %s ", jsonString(task), time.Now().String())
					}
				}
			}
		} else if !c.Reduced {
			for i, task := range c.ReduceTask {
				if task.Status == WorkerStatusTypeRunning {
					if time.Since(task.Timestamp) > 10*time.Second {
						c.ReduceTask[i].Status = WorkerStatusWaiting
						c.log("reduceTask %+v is timeout in %s ", jsonString(task), time.Now().String())
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

	c.log("finished map,the reduce file is:")
	c.log(jsonString(c.ReduceTask))
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

func (c *Coordinator) Task(args *Req, reply *Resp) error {
	c.Lock()
	defer c.Unlock()
	c.log("receive:%s", jsonString(*args))
	if args.Task.Index != -1 {
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
	c.log("reply:%s", jsonString(*reply))
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
