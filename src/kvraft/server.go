package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func (kv *KVServer) log(str string, args ...interface{}) {
	if Debug {
		log.Printf(
			"%v,%+v",
			fmt.Sprintf(
				fmt.Sprintf(
					"kvServer(id:%v,lastIndex:%v,%v)##:", kv.me, kv.lastIndex,
					str,
				),
				args...,
			), "",
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
	ClientId string
	SeqId    int
	OpType   OpType
	Key      string
	Value    string
	Index    int
	Term     int

	Result string
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type opMap struct {
	m map[string]chan nullStruct
	sync.RWMutex
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	persister    *raft.Persister
	data         map[string]string
	opMp         sync.Map
	reqMp        map[string]int
	lastIndex    int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.log("recv args:%v", args)
	op := Op{
		OpType:   GET,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		//Value:  args.Value,
	}
	index, isOld, isLeader := kv.startOp(op)
	if isOld {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Err = ErrIgnored
		reply.Value = kv.data[args.Key]
		return
	}
	if isLeader {
		opMsg, ok := kv.opMp.Load(index)
		if !ok {
			panic(ErrFailed)
		}
		kv.log("%+v wait Index %v/%v", args, kv.lastIndex, index)
		select {
		case op := <-opMsg.(chan Op):
			kv.mu.Lock()
			defer kv.mu.Unlock()
			kv.log("get Msg:%+v,%+v", args, reply, index)
			if op.SeqId != args.SeqId || op.ClientId != args.ClientId {
				reply.Err = ErrFailed
				kv.log("getFailed:%+v,%+v", args, reply, index)
			}
			if v, ok := kv.data[args.Key]; ok {
				reply.Value = v
				reply.Err = OK
				kv.log("getSuccess:%+v,%+v", args, reply, index)
			} else {
				reply.Err = ErrNoKey
			}
			return
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applier() {
	kv.rf.GetState()
	for ch := range kv.applyCh {
		func(ch raft.ApplyMsg) {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if ch.SnapshotValid {
				kv.log("will applier snapshot,%+v", ch)
				if kv.rf.CondInstallSnapshot(ch.SnapshotTerm, ch.SnapshotIndex, ch.Snapshot) {
					kv.log("applier snapshot,%+v", ch)
					by := bytes.NewReader(ch.Snapshot)
					de := labgob.NewDecoder(by)
					if !(de.Decode(&kv.data) == nil && de.Decode(&kv.reqMp) == nil) {
						panic("sync snapshot error")
					}
					kv.lastIndex = ch.SnapshotIndex
				}
			}
			if ch.CommandValid {
				op, ok := ch.Command.(Op)
				if !ok {
					return
				}
				kv.log("applier!!,%+v", ch)
				if v, ok := kv.reqMp[op.ClientId]; !ok || op.SeqId > v {
					kv.reqMp[op.ClientId] = op.SeqId
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
				}
				kv.log("#######SIZE:%v", kv.rf.GetStateSize())
				if ch.CommandIndex != 0 && kv.rf.GetStateSize() > kv.maxraftstate {
					kv.log("will create snapshot,%+v", ch)
					by := new(bytes.Buffer)
					e := labgob.NewEncoder(by)
					if e.Encode(kv.data) == nil && e.Encode(kv.reqMp) == nil {
						kv.rf.Snapshot(ch.CommandIndex, by.Bytes())
					} else {
						panic("create snapshot error")
					}
				}
				if v, ok := kv.opMp.Load(ch.CommandIndex); ok {
					go func() {
						kv.log("applier success!!,%+v,index opt found in opStatus", ch)
						v.(chan Op) <- op
					}()
				}
			}
		}(ch)

	}
}

type nullStruct struct {
}

type opMSg struct {
	term        int
	index       int
	failedChan  chan nullStruct
	successChan chan nullStruct
}

func (kv *KVServer) startOp(op Op) (int, bool, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//这里对于过时的get请求，查出来的数据有可能不线性
	if v, ok := kv.reqMp[op.ClientId]; ok && op.SeqId <= v {
		return 0, true, false
	}
	if _, isL := kv.rf.GetState(); !isL {
		return 0, false, false
	}
	index, _, isLeader := kv.rf.Start(op)
	kv.opMp.Store(index, make(chan Op))

	return index, false, isLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.log("recv args:%v", args)
	op := Op{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		OpType:   OpType(args.Op),
		Key:      args.Key,
		Value:    args.Value,
	}
	index, isOld, isLeader := kv.startOp(op)
	if isOld {
		reply.Err = ErrIgnored
		return
	}
	if isLeader {
		opMsg, ok := kv.opMp.Load(index)
		if !ok {
			panic(ErrFailed)
		}
		kv.log("%+v wait Index %v/%v", args, kv.lastIndex, index)
		select {
		case op := <-opMsg.(chan Op):
			if op.SeqId != args.SeqId || op.ClientId != args.ClientId {
				kv.log("seq too old:%+v,%+v,%+v", args, reply, index)
				reply.Err = ErrFailed
			} else {
				kv.log("putSuccess:%+v,%+v,%+v", args, reply, index)
				reply.Err = OK
			}
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

func (kv *KVServer) loadSnapshot(persister *raft.Persister) {
	if persister.SnapshotSize() > 0 {
		by := bytes.NewReader(persister.ReadSnapshot())
		de := labgob.NewDecoder(by)
		if !(de.Decode(&kv.data) == nil && de.Decode(&kv.reqMp) == nil) {
			panic("sync snapshot error")
		}
	}
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
	kv.reqMp = make(map[string]int)
	kv.persister = persister
	//kv.opStatus = sync.Map{}
	//kv.opStatus.RWMutex = sync.RWMutex{}
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.loadSnapshot(persister)
	//c:=sync.map{}
	//time.Sleep(5 * time.Second)
	//You may need initialization code here.
	go kv.applier()
	return kv
}
