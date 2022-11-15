package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)
import "net/rpc"
import "hash/fnv"

//mev
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

var logFile io.Writer

func logf(str string, args ...interface{}) {
	fmt.Fprintf(logFile, str+"\n", args...)
}

//
// main/mrworker.go calls this function.
//
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	f, err := os.OpenFile("mr-logfile"+strconv.Itoa(os.Getpid())+".log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	logFile = f

	log.SetFlags(log.Lshortfile | log.Llongfile)
	req := &Req{Task: Task{Type: TaskTypeNULL, Index: -1}}
	resp := &Resp{}
	if ok := call("Coordinator.Task", req, resp); !ok {
		time.Sleep(time.Millisecond * 10)
	}
	for {
		req.Task = resp.Task
		switch resp.Task.Type {
		case TaskTypeNULL:
			time.Sleep(1 * time.Second)
		case TaskShutdown:
			fmt.Printf("shutdown")
			os.Exit(0)
		case TaskTypeMap:
			DoMap(resp.NReduce, &resp.Task, mapf)
			req.Task = resp.Task
			req.Task.Status = WorkerStatusDone
		case TaskTypeReduce:
			DoReduce(resp.NReduce, &resp.Task, reducef)
			req.Task = resp.Task
			req.Task.Status = WorkerStatusDone
		}
		logf("req:%s", jsonString(*req))
		resp = &Resp{}
		if ok := call("Coordinator.Task", req, resp); !ok {
			log.Printf("return %+v error", req.Task)
		}
		logf("resp:%s", jsonString(*resp))
	}

}
func DoReduce(nReduce int, task *Task, reducef func(string, []string) string) {
	mp := make(map[string][]string)
	for _, fileName := range task.ReduceFiles {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := mp[kv.Key]; !ok {
				mp[kv.Key] = []string{}
			}
			mp[kv.Key] = append(mp[kv.Key], kv.Value)
		}
	}
	for k, values := range mp {
		fileName := fmt.Sprintf("mr-out-%d", ihash(k)%nReduce)
		v := reducef(k, values)
		f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		defer f.Close()
		if err != nil {
			log.Fatal(err)
		}
		_, err = fmt.Fprintf(f, "%v %v\n", k, v)
		if err != nil {
			log.Fatal(err)
		}
	}
}
func DoMap(nReduce int, task *Task, mapf func(string, string) []KeyValue) {
	fileContent, err := os.ReadFile(task.MapFile)
	if err != nil {
		log.Fatal(err)
	}
	rsp := mapf(task.MapFile, string(fileContent))
	fileIN := make(map[string]*json.Encoder)
	by, _ := json.Marshal(rsp)
	logf("mapf result,%s", string(by))
	os.Mkdir("tmp", os.ModePerm)
	for _, kv := range rsp {
		rFileName := fmt.Sprintf("mr-%d-%d", task.Index, ihash(kv.Key)%nReduce)
		fileName := "tmp/" + rFileName
		if _, ok := fileIN[rFileName]; !ok {
			f, err := os.Create(fileName)
			defer f.Close()
			if err != nil {
				log.Fatal(err)
			} else {
				fileIN[rFileName] = json.NewEncoder(f)
			}
		}
		err := fileIN[rFileName].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
	task.ReduceFiles = []string{}
	for s, _ := range fileIN {
		task.ReduceFiles = append(task.ReduceFiles, s)
	}
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
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
	log.Println(err)
	return false
}
