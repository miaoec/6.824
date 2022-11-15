package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/json"
	"log"
	"os"
	"time"
)
import "strconv"

type TaskType int

const (
	TaskTypeNULL TaskType = iota
	TaskTypeMap
	TaskTypeReduce
	TaskShutdown
)

type Task struct {
	Type        TaskType         // "Map", "Reduce", "Wait"
	Status      WorkerStatusType // "Unassigned", "Assigned", "Finished"
	Index       int              // Index of the task
	Timestamp   time.Time        // Start time
	MapFile     string           // File for map task
	ReduceFiles []string         // List of files for reduce task
}

type Req struct {
	Task Task
}

type Resp struct {
	Task    Task
	NReduce int
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func jsonString(o interface{}) string {
	by, err := json.Marshal(o)
	if err != nil {
		log.Fatal(err)
	}
	return string(by)
}
