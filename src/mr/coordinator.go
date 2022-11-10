package mr

import "C"
import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
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

func (c *Coordinator) popData(size int) (key string, value []string) {
	for s, v := range c.MappedData {
		if len(v) == 1 {
			continue
		}
		key, value = s, c.MappedData[s]
		c.MappedData[s] = []string{}
		return
	}
	return "", nil
}

func (c *Coordinator) pushData(key string, value ...string) {
	if _, ok := c.MappedData[key]; ok {
		c.MappedData[key] = append(c.MappedData[key], value...)
	} else {
		c.MappedData[key] = value
	}
}

type Coordinator struct {
	Files   []KeyValue
	nReduce int
	//MappedData    []KeyValue
	Mapped     bool
	Reduced    bool
	MappedData map[string][]string
	ReduceData map[string][]string
	WorkStatus []*WorkerStatus
	reduceSize int
	sync.Mutex
}

type WorkerStatusType int32

const (
	WorkerStatusTypeRunning WorkerStatusType = iota
	WorkerStatusDead
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
func (c *Coordinator) reduceTask(reply *Resp) {

	if len(c.MappedData) != 0 {
		key, value := c.popData(c.reduceSize)
		reply.ReduceData = ReduceData{Key: key, Value: value}
		reply.Type = TaskTypeReduce
		reply.Offset = len(c.WorkStatus)
		c.WorkStatus = append(
			c.WorkStatus, &WorkerStatus{
				Resp:         *reply,
				Status:       WorkerStatusTypeRunning,
				lastCallTime: time.Now(),
			},
		)
	} else {
		reply.Type = TaskTypeNULL
	}
	return
}

func (c *Coordinator) mapTask(reply *Resp) {
	if len(c.Files) > 0 {
		reply.MapData = c.Files[0]
		c.Files = c.Files[1:]
		reply.Type = TaskTypeMap
		reply.Offset = len(c.WorkStatus)
		c.WorkStatus = append(
			c.WorkStatus, &WorkerStatus{
				Resp:         *reply,
				Status:       WorkerStatusTypeRunning,
				lastCallTime: time.Now(),
			},
		)
	} else {
		reply.Type = TaskTypeNULL
	}
}

func (c *Coordinator) checkMapped() bool {
	if !c.Mapped {
		if len(c.Files) == 0 {
			for offset, status := range c.WorkStatus {
				if status.Status == WorkerStatusTypeRunning {
					return false
				}
				if status.Status == WorkerStatusDead {
					continue
				}
				if time.Since(status.lastCallTime) > time.Second*2 {
					log.Printf("WorkerStatusDead offset=%+v", offset)
					switch status.Type {
					case TaskTypeMap:
						c.Files = append(c.Files, status.MapData)
					case TaskTypeReduce:
						c.pushData(status.ReduceData.Key, status.ReduceData.Value...)
					}
					c.WorkStatus[offset].Status = WorkerStatusDead
				}
			}
			c.Mapped = true
		}
	}
	return c.Mapped
}

func (c *Coordinator) checkReduced() bool {
	if !c.Mapped {
		return false
	}
	if !c.Reduced {
		hasRunning := false
		for offset, status := range c.WorkStatus {
			if status.Status == WorkerStatusTypeRunning {
				hasRunning = true
				if time.Since(status.lastCallTime) > time.Second*5 {
					log.Printf("WorkerStatusDead offset=%+v", offset)
					switch status.Type {
					case TaskTypeMap:
						c.Files = append(c.Files, status.MapData)
					case TaskTypeReduce:
						c.pushData(status.ReduceData.Key, status.ReduceData.Value...)
					}
					c.WorkStatus[offset].Status = WorkerStatusDead
				}
			}
		}
		if hasRunning {
			return false
		}
		for _, s := range c.MappedData {
			if len(s) > 1 {
				return false
			}
		}
		c.Reduced = true

	}
	return c.Reduced
}

func (c *Coordinator) Task(args *Req, reply *Resp) error {
	c.Lock()
	defer c.Unlock()
	if args.Offset != -1 && c.WorkStatus[args.Offset].Status != WorkerStatusDead {
		switch args.Type {
		case TaskTypeReduce:
			c.pushData(args.ReducedData.Key, args.ReducedData.Value)
			c.WorkStatus[args.Offset].Status = WorkerStatusDone
		case TaskTypeMap:
			for _, datum := range args.MappedData {
				if _, ok := c.MappedData[datum.Key]; ok {
					c.MappedData[datum.Key] = append(c.MappedData[datum.Key], datum.Value)
				} else {
					c.MappedData[datum.Key] = []string{datum.Value}
				}
			}

			c.WorkStatus[args.Offset].Status = WorkerStatusDone
		}
	}
	if !c.checkMapped() {
		c.mapTask(reply)
	} else if !c.checkReduced() {
		c.reduceTask(reply)
	} else {
		fmt.Println("FIN")
	}
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
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
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
	c.Lock()
	defer c.Unlock()
	return c.Reduced
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		c.Files = append(
			c.Files,
			KeyValue{Key: filename, Value: string(content)},
		)
	}
	c.nReduce = nReduce
	c.reduceSize = 2
	c.MappedData = map[string][]string{}
	c.ReduceData = map[string][]string{}

	// Your code here.

	c.server()
	return &c
}
