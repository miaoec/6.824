package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func (kv *KVServer) log(str string, args ...interface{}) {
	//_, isLeader := kv.rf.GetState()
	if isDebug {
		log.Printf(
			"%v,%+v",
			fmt.Sprintf(
				fmt.Sprintf(
					"kvServer(id:%v,lastIndex:%v,%v)##:", kv.me, kv.lastIndex,
					str,
				),
				args...,
			), kv.data,
		)
	}

}

type OpType string

const (
	PUT    OpType = "Put"
	GET           = "Get"
	APPEND        = "Append"
)

type Op struct {
	RequestID string
	OpType    OpType
	Key       string
	Value     string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu            sync.Mutex
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	dead          int32 // set by Kill()
	maxraftstate  int   // snapshot if log grows this big
	data          map[string]string
	opStatus      map[string]chan opMsg
	opIndexStatus map[string]chan opMsg
	lastIndex     int64
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	op := Op{
		OpType:    GET,
		Key:       args.Key,
		RequestID: args.RequestID,
		//Value:  args.Value,
	}
	index, isLeader := kv.startOp(op)

	if isLeader {
		kv.log("%+v wait Index %v/%v", args, kv.lastIndex, index)
		select {
		case <-kv.opStatus[op.RequestID]:
			kv.mu.Lock()
			kv.log("putSuccess:%+v,%+v,%+v", args, reply, index)
			if v, ok := kv.data[args.Key]; ok {
				reply.Value = v
				reply.Err = OK
				kv.log("getSuccess:%+v,%+v,%+v", args, reply, index)
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applier() {
	kv.rf.GetState()
	for ch := range kv.applyCh {
		if ch.CommandValid {
			kv.mu.Lock()
			op, ok := ch.Command.(Op)
			if !ok {
				continue
			}
			kv.log("applier!!,%+v", ch)
			switch op.OpType {
			case PUT:
				kv.data[op.Key] = op.Value
			case APPEND:
				if _, ok := kv.data[op.Key]; !ok {
					kv.data[op.Key] = op.Value
				} else {
					kv.data[op.Key] += op.Value
				}
			}
			kv.lastIndex = int64(ch.CommandIndex)
			_, ok = kv.opStatus[op.RequestID]
			kv.mu.Unlock()
			if ok {
				go func() {
					kv.opStatus[op.RequestID] <- opMsg{}
					kv.log("applier success!!,%+v,index opt found in opStatus", ch)
				}()
			} else {
				kv.log("applier error!!,%+v,index opt found in opStatus", ch)
			}

		}
	}
}

type opMsg struct {
}

func (kv *KVServer) startOp(op Op) (int, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)
	kv.opStatus[op.RequestID] = make(chan opMsg)
	return index, isLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		RequestID: args.RequestID,
		OpType:    OpType(args.Op),
		Key:       args.Key,
		Value:     args.Value,
	}
	index, isLeader := kv.startOp(
		op,
	)
	if isLeader {
		kv.log("%+v wait Index %v/%v", args, kv.lastIndex, index)
		select {
		case <-kv.opStatus[op.RequestID]:
			kv.log("putSuccess:%+v,%+v,%+v", args, reply, index)
			reply.Err = OK
		}
	} else {
		reply.Err = ErrWrongLeader
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.data = make(map[string]string)
	kv.opStatus = make(map[string]chan opMsg)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//time.Sleep(5 * time.Second)
	//You may need initialization code here.
	go kv.applier()
	return kv
}
