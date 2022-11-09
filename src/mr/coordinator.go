package mr

import (
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
	Files      []string
	nReduce    int32
	MapData    []string
	Mapped     bool
	Reduced    bool
	ReduceData []string
	WorkStatus map[int32]WorkerStatus
	reduceSize int
	sync.Mutex
}

type WorkerStatusType int32

const (
	WorkerStatusTypeRunning WorkerStatusType = iota
	WorkerStatusTypeReady
	WorkerStatusDead
)

type WorkerStatus struct {
	Resp
	WorkerStatusType
	lastCallTime time.Time
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func (c *Coordinator) mapTask(args *Req, reply *Resp) error {
	c.Lock()
	defer c.Unlock()
	c.ReduceData = append(c.ReduceData, args.ReduceData)
	if len(c.ReduceData) != 0 {
		reply.ReduceData = c.ReduceData[:min(c.reduceSize, len(c.ReduceData))]
		reply.Type = TaskTypeReduce
	}
	return nil
}

func (c *Coordinator) reduceTask(args *Req, reply *Resp) error {
	c.Lock()
	defer c.Unlock()
	c.MapData = append(c.MapData, args.MapData...)
	if len(c.Files) != 0 {
		reply.MapData = c.Files[0]
		c.Files = append(c.Files[:0], c.Files[1:]...)
		reply.Type = TaskTypeReduce
		c.WorkStatus[args.WorkerId] = WorkerStatus{
			Resp:             *reply,
			WorkerStatusType: WorkerStatusTypeRunning,
			lastCallTime:     time.Now(),
		}
	} else {
		reply.Type = TaskTypeNULL
	}
	return nil
}

func (c *Coordinator) Task(args *Req, reply *Resp) error {
	switch {

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

func (c *Coordinator) Map(args *Args, reply *Reply) error {
	return nil
}

func (c *Coordinator) Reduce(args *Args, reply *Reply) error {
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

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
