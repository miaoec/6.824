package raftsvr

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"github.com/pkg/errors"
	"strconv"
	"sync"
	"sync/atomic"
)

type RaftSVr struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this bigapplier!!
	persister    *raft.Persister
	data         map[string]interface{}
	opMp         sync.Map
	reqMp        map[string]int
	lastIndex    int
}

var OpMap = map[OpType]OpFunc{}
var ErrOpNotMatch = errors.New("error Op not match")

type OpType string
type OpFunc func(sc *RaftSVr, op Op) (interface{}, error)

func (sc *RaftSVr) Do(op *Op, reply *Reply) {
	index, isOld, isLeader := sc.startOp(*op)
	defer func() {
		reply.ServerId = strconv.Itoa(sc.me)
	}()
	if isOld {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		reply.Err = ErrIgnored
		return
	}
	if isLeader {
		opMsg, ok := sc.opMp.Load(index)
		if !ok {
			panic(ErrFailed)
		}
		select {
		case result := <-opMsg.(chan Reply):
			reply.CopyFrom(result)
			if op.SeqId != result.SeqId || op.ClientId != result.ClientId {
				reply.Err = ErrFailed
			}
			return
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sc *RaftSVr) applier() {
	for !sc.killed() {
		select {
		case ch := <-sc.applyCh:
			func(ch raft.ApplyMsg) {
				sc.mu.Lock()
				defer sc.mu.Unlock()
				if ch.SnapshotValid {
					if sc.rf.CondInstallSnapshot(ch.SnapshotTerm, ch.SnapshotIndex, ch.Snapshot) {
						by := bytes.NewReader(ch.Snapshot)
						de := labgob.NewDecoder(by)
						if !(de.Decode(&sc.data) == nil && de.Decode(&sc.reqMp) == nil) {
							panic("sync snapshot error")
						}
						sc.lastIndex = ch.SnapshotIndex
					}
				}
				if ch.CommandValid {
					op, ok := ch.Command.(Op)
					if !ok {
						return
					}
					if v, ok := sc.reqMp[op.ClientId]; !ok || op.SeqId > v {
						sc.reqMp[op.ClientId] = op.SeqId
						if f, ok := OpMap[op.OpType]; ok {
							reply := Reply{
								SeqId:    op.SeqId,
								ClientId: op.ClientId,
								ServerId: strconv.Itoa(sc.me),
								Err:      OK,
							}
							result, err := f(sc, op)
							reply.Result = result
							if err != nil {
								reply.Err = err.Error()
							}
							if v, ok := sc.opMp.Load(ch.CommandIndex); ok {
								go func() {
									v.(chan Reply) <- reply
								}()
							}
						} else {
							panic(ErrOpNotMatch)
						}
					}
					//sc.log("#######SIZE:%v", sc.rf.GetStateSize())
					if ch.CommandIndex != 0 && sc.maxraftstate != -1 && sc.rf.GetStateSize() > sc.maxraftstate {
						by := new(bytes.Buffer)
						e := labgob.NewEncoder(by)
						if e.Encode(sc.data) == nil && e.Encode(sc.reqMp) == nil {
							sc.rf.Snapshot(ch.CommandIndex, by.Bytes())
						} else {
							panic("create snapshot error")
						}
					}

				}
			}(ch)

		}
	}
}

func (sc *RaftSVr) IsOld(op Op) bool {
	if v, ok := sc.reqMp[op.ClientId]; ok && op.SeqId <= v {
		return true
	}
	return false
}

func (sc *RaftSVr) startOp(op Op) (int, bool, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	//这里对于过时的get请求，查出来的数据有可能不线性
	if v, ok := sc.reqMp[op.ClientId]; ok && op.SeqId <= v {
		return 0, true, false
	}
	if _, isL := sc.rf.GetState(); !isL {
		return 0, false, false
	}
	index, _, isLeader := sc.rf.Start(op)
	sc.opMp.Store(index, make(chan Reply))
	return index, false, isLeader
}

func (sc *RaftSVr) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *RaftSVr) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *RaftSVr) loadSnapshot(persister *raft.Persister) {
	if persister.SnapshotSize() > 0 {
		by := bytes.NewReader(persister.ReadSnapshot())
		de := labgob.NewDecoder(by)
		if !(de.Decode(&sc.data) == nil && de.Decode(&sc.reqMp) == nil) {
			panic("sync snapshot error")
		}
	}
}

//func (sc *RaftSVr) Join(args *JoinArgs, reply *JoinReply) {
//	// Your code here.
//}
//
//func (sc *RaftSVr) Leave(args *LeaveArgs, reply *LeaveReply) {
//	// Your code here.
//}
//
//func (sc *RaftSVr) Move(args *MoveArgs, reply *MoveReply) {
//	// Your code here.
//}
//
//func (sc *RaftSVr) Query(args *QueryArgs, reply *QueryReply) {
//	// Your code here.
//}

// needed by shardsc tester
func (sc *RaftSVr) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant RaftSVr service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *RaftSVr {

	labgob.Register(Op{})
	labgob.Register(Reply{})
	sc := new(RaftSVr)
	sc.me = me
	sc.maxraftstate = -1

	sc.reqMp = make(map[string]int)
	sc.persister = persister
	sc.applyCh = make(chan raft.ApplyMsg)

	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	//sc.loadSnapshot(persister)
	//You may need initialization code here.
	//sc.
	go sc.applier()
	return sc
}
