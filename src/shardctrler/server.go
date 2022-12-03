package shardctrler

import (
	"6.824/raft"
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
)
import "6.824/labrpc"
import "6.824/labgob"

type ShardCtrler struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this bigapplier!!
	lastBan      *Config
	persister    *raft.Persister
	data         map[string]interface{}
	opMp         sync.Map
	reqMp        map[string]int
	lastIndex    int

	configs []Config
}

func (sc *ShardCtrler) log(str string, args ...interface{}) {
	return
	if ServerDebug && sc != nil {
		log.Printf(
			"%v",
			fmt.Sprintf(
				fmt.Sprintf(
					"scServer(id:%v,lastIndex:%v,%v)##:", sc.me, sc.lastIndex,
					str,
				),
				args...,
			),
		)
	}
}

func setLatestIndex(sc *ShardCtrler, index int) {
	sc.data["lastIndex"] = index
}

func getLatestIndex(sc *ShardCtrler) int {
	if v, ok := sc.data["lastIndex"]; ok {
		return v.(int)
	}
	return 0
}

func getIndexConf(sc *ShardCtrler, index string) (Config, error) {
	if v, ok := sc.data[index]; ok {
		return v.(Config), nil
	}
	return Config{}, ErrKeyNotFound
}

var OpMap = map[OpType]OpFunc{
	Join: func(sc *ShardCtrler, op Op) (interface{}, error) {
		lastIndex := getLatestIndex(sc)
		conf, _ := getIndexConf(sc, strconv.Itoa(lastIndex))
		newGroup := make(map[int][]string)
		c, _ := op.Value.(JoinArgs)
		for i, ints := range conf.Groups {
			newGroup[i] = ints
		}
		for i, ints := range c.Servers {
			newGroup[i] = ints
		}
		newConfig := Config{Shards: conf.Shards, Groups: newGroup, Num: lastIndex + 1}
		newConfig.ReBalance(sc, map[int]int{})
		sc.data[strconv.Itoa(lastIndex+1)] = newConfig
		setLatestIndex(sc, lastIndex+1)
		return nil, nil
	},

	Query: func(sc *ShardCtrler, op Op) (interface{}, error) {
		args := op.Value.(QueryArgs)
		config, err := getIndexConf(sc, strconv.Itoa(args.Num))
		if err == ErrKeyNotFound {
			config, _ = getIndexConf(sc, strconv.Itoa(getLatestIndex(sc)))
		}
		return config, nil
	},
	Leave: func(sc *ShardCtrler, op Op) (interface{}, error) {
		lastIndex := getLatestIndex(sc)
		conf, _ := getIndexConf(sc, strconv.Itoa(lastIndex))
		newGroup := make(map[int][]string)
		leavSet := map[string]struct{}{}
		c, _ := op.Value.(LeaveArgs)
		for _, s := range c.GIDs {
			leavSet[strconv.Itoa(s)] = struct{}{}
		}
		for i, ints := range conf.Groups {
			if _, ok := leavSet[strconv.Itoa(i)]; !ok {
				newGroup[i] = ints
			}
		}
		newConfig := Config{Shards: conf.Shards, Groups: newGroup, Num: lastIndex + 1}
		newConfig.ReBalance(sc, map[int]int{})
		sc.data[strconv.Itoa(lastIndex+1)] = newConfig
		setLatestIndex(sc, lastIndex+1)
		return nil, nil
	},
	Move: func(sc *ShardCtrler, op Op) (interface{}, error) {
		lastIndex := getLatestIndex(sc)
		conf, _ := getIndexConf(sc, strconv.Itoa(lastIndex))
		c := op.Value.(MoveArgs)
		newConfig := Config{Shards: conf.Shards, Groups: conf.Groups, Num: lastIndex + 1}
		newConfig.Shards[c.Shard] = c.GID
		newConfig.ReBalance(sc, map[int]int{c.Shard: c.GID})
		sc.data[strconv.Itoa(lastIndex+1)] = newConfig
		setLatestIndex(sc, lastIndex+1)
		return nil, nil
	},
}
var ErrKeyNotFound = errors.New("ErrNoKey")
var ErrOpNotMatch = errors.New("error Op not match")

const (
	Join  OpType = "Join"
	Leave        = "Leave"
	Query        = "Query"
	Move         = "Move"
)

type OpType string
type OpFunc func(sc *ShardCtrler, op Op) (interface{}, error)

func (sc *ShardCtrler) Do(op *Op, reply *Reply) {
	index, isOld, isLeader := sc.startOp(*op)
	defer func() {
		reply.ServerId = strconv.Itoa(sc.me)
	}()
	if isOld {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		reply.Err = ErrIgnored
		//if op.OpType == GET {
		//	reply.Result = sc.data[op.Key]
		//}
		return
	}
	if isLeader {
		opMsg, ok := sc.opMp.Load(index)
		if !ok {
			panic(ErrFailed)
		}
		sc.log("%+v wait Index %v/%v", op, sc.lastIndex, index)
		select {
		case result := <-opMsg.(chan Reply):
			sc.log("get Msg:%+v,%+v,%v", result, op, index)
			reply.CopyFrom(result)
			if op.SeqId != result.SeqId || op.ClientId != result.ClientId {
				reply.Err = ErrFailed
				sc.log("getFailed:%+v,%+v", op, reply, index)
			}
			return
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case ch := <-sc.applyCh:
			func(ch raft.ApplyMsg) {
				sc.mu.Lock()
				defer sc.mu.Unlock()
				if ch.SnapshotValid {
					sc.log("will applier snapshot,%+v", ch)
					if sc.rf.CondInstallSnapshot(ch.SnapshotTerm, ch.SnapshotIndex, ch.Snapshot) {
						sc.log("applier snapshot,%+v", ch)
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
					sc.log("applier!!,%+v", ch)
					if v, ok := sc.reqMp[op.ClientId]; !ok || op.SeqId > v {
						sc.reqMp[op.ClientId] = op.SeqId
						if f, ok := OpMap[op.OpType]; ok {
							reply := Reply{
								SeqId:    op.SeqId,
								ClientId: op.ClientId,
								//Result:   result,
								ServerId: strconv.Itoa(sc.me),
								Err:      OK,
							}
							result, err := f(sc, op)
							reply.Result = result
							if err != nil {
								sc.log("reply error%+v", err)
								reply.Err = err.Error()
							}

							if v, ok := sc.opMp.Load(ch.CommandIndex); ok {
								go func() {
									sc.log("applier success!!,%+v,index opt found in opStatus", ch)
									v.(chan Reply) <- reply
								}()
							}
						} else {
							panic(ErrOpNotMatch)
						}
					}
					//sc.log("#######SIZE:%v", sc.rf.GetStateSize())
					if ch.CommandIndex != 0 && sc.maxraftstate != -1 && sc.rf.GetStateSize() > sc.maxraftstate {
						sc.log("will create snapshot,%+v", ch)
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

func (sc *ShardCtrler) IsOld(op Op) bool {
	if v, ok := sc.reqMp[op.ClientId]; ok && op.SeqId <= v {
		return true
	}
	return false
}

func (sc *ShardCtrler) startOp(op Op) (int, bool, bool) {
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

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) loadSnapshot(persister *raft.Persister) {
	if persister.SnapshotSize() > 0 {
		by := bytes.NewReader(persister.ReadSnapshot())
		de := labgob.NewDecoder(by)
		if !(de.Decode(&sc.data) == nil && de.Decode(&sc.reqMp) == nil) {
			panic("sync snapshot error")
		}
	}
}

//func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
//	// Your code here.
//}
//
//func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
//	// Your code here.
//}
//
//func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
//	// Your code here.
//}
//
//func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
//	// Your code here.
//}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {

	labgob.Register(Op{})
	labgob.Register(Reply{})
	labgob.Register(QueryArgs{})
	labgob.Register(Config{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(JoinArgs{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.maxraftstate = -1
	sc.data = map[string]interface{}{
		"lastIndex": 0,
		"0": Config{
			Groups: map[int][]string{},
		},
	}

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
