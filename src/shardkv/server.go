package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sasha-s/go-deadlock"
	"log"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Shard struct {
	Data   map[string]interface{}
	Status ShardStatus
}

func (s Shard) deepCopy() Shard {
	data := make(map[string]interface{})
	if s.Data != nil {
		for s2, i := range s.Data {
			data[s2] = i
		}
	}
	return Shard{Data: data, Status: s.Status}
}

type ShardStatus string

const (
	ShardStatusRunning ShardStatus = "Running"
	ShardStatusPulling ShardStatus = "Pulling"
	ShardStatusErasing ShardStatus = "Erasing"
	ShardStatusGCing   ShardStatus = "GCing"
	ShardStatusInvalid ShardStatus = "Invalid"
)

type ShardKV struct {
	mu       deadlock.RWMutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) *labrpc.ClientEnd
	gid      int
	ctrlers  []*labrpc.ClientEnd
	sm       *shardctrler.Clerk
	config   shardctrler.Config //persist
	//config_mux   sync.Mutex
	lastConfig   shardctrler.Config //persist
	maxraftstate int                // snapshot if log grows this big
	dead         int32
	lastIndex    int
	data         map[int]*Shard //persist
	reqMp        map[string]int //persist
	opMp         sync.Map
	pullCount    int
	// Your definitions here.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send

func (kv *ShardKV) name() string {
	return strconv.Itoa(kv.gid) + "-" + strconv.Itoa(kv.me)
}

func StartServer(
	servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int,
	ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd,
) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	//log.SetFlags(log.Lshortfile | log.Ldate | log.Lmicroseconds)
	labgob.Register(Command{})
	labgob.Register(Op{})
	labgob.Register(Reply{})
	labgob.Register(PullShardArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GCArgs{})
	labgob.Register(GCReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.config = shardctrler.Config{Num: 0}
	kv.lastConfig = shardctrler.Config{Num: -1}
	kv.data = make(map[int]*Shard)

	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.reqMp = make(map[string]int)
	if persister.SnapshotSize() > 0 {
		kv.loadSnapshot(persister.ReadSnapshot())
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.LogName = kv.name()
	time.Sleep(100 * time.Millisecond)

	go pprof.Do(
		context.Background(), pprof.Labels("path", "/api/applier", "kvID", kv.name()),
		kv.applier,
	)
	go pprof.Do(
		context.Background(), pprof.Labels("path", "/api/syncConfig", "kvID", kv.name()),
		kv.syncConfig,
	)
	go pprof.Do(
		context.Background(), pprof.Labels("path", "/api/pullingShard", "kvID", kv.name()),
		kv.pullingShard,
	)
	//go kv.gcing()

	return kv
}

func (kv *ShardKV) loadSnapshot(snapShot []byte) {
	if len(snapShot) > 0 {
		by := bytes.NewReader(snapShot)
		de := labgob.NewDecoder(by)
		if !(de.Decode(&kv.data) == nil && de.Decode(&kv.reqMp) == nil &&
			de.Decode(&kv.config) == nil && de.Decode(&kv.lastConfig) == nil) {
			panic("sync snapshot error")
		}
	}
}

type OpType string

type CommandType string

//type OpCase

type OpFunc func(kv *ShardKV, op Op) (interface{}, error)

var OpNameMap = map[OpType]OpFunc{
	GET: func(kv *ShardKV, op Op) (interface{}, error) {
		if value, ok := kv.data[key2shard(op.Key)].Data[op.Key]; ok {
			return value, nil
		}
		return "", errors.New(ErrNoKey)
	},
	PUT: func(kv *ShardKV, op Op) (interface{}, error) {
		kv.data[key2shard(op.Key)].Data[op.Key] = op.Value
		return nil, nil
	},
	APPEND: func(kv *ShardKV, op Op) (interface{}, error) {
		if _, ok := kv.data[key2shard(op.Key)].Data[op.Key]; !ok {
			kv.data[key2shard(op.Key)].Data[op.Key] = op.Value
		} else {
			if v, ok := kv.data[key2shard(op.Key)].Data[op.Key].(string); ok {
				kv.data[key2shard(op.Key)].Data[op.Key] = v + op.Value
			} else {
				return nil, errors.New("OpFuncAppend Error,dataType not str")
			}
		}
		return nil, nil
	},
}

func (kv *ShardKV) applier(ctx context.Context) {
	for !kv.killed() {
		select {
		case ch := <-kv.applyCh:
			func(ch raft.ApplyMsg) {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				if ch.SnapshotValid {
					kv.applySnapshot(ch)
				}
				if ch.CommandValid {
					kv.applyCommand(ch)

				}
			}(ch)

		}
	}
}
func (kv *ShardKV) log(str string, args ...interface{}) {
	_, isL := kv.rf.GetState()
	state := "Follower"
	if isL {
		state = "Leader"
	}
	if ServerDebug && kv != nil {
		log.Printf(
			"%v",
			fmt.Sprintf(
				fmt.Sprintf(
					"kvServer(id:%v-%v,%v,%v)##:", kv.gid, kv.me, state,
					str,
				),
				args...,
			),
		)
	}
}
func (kv *ShardKV) startCommand(cm Command) (int, bool) {
	index, _, isLeader := kv.rf.Start(cm)
	kv.opMp.Store(index, make(chan Reply))
	return index, isLeader
}

func (kv *ShardKV) startOp(op Op) (int, bool, bool) {

	//这里对于过时的get请求，查出来的数据有可能不线性
	if v, ok := kv.reqMp[op.ClientId]; ok && op.SeqId <= v {
		return 0, true, false
	}
	if _, isL := kv.rf.GetState(); !isL {
		return 0, false, false
	}
	index, _, isLeader := kv.rf.Start(Command{CommandType: LogCommand, LogCommand: op})
	kv.opMp.Store(index, make(chan Reply))
	return index, false, isLeader
}
func (kv *ShardKV) Do(op *Op, reply *Reply) {
	//kv.log("reciv op:%+v", *op)
	kv.mu.Lock()
	if kv.config.Shards[key2shard(op.Key)] != kv.gid || kv.data[key2shard(op.Key)].Status != ShardStatusRunning {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	index, isOld, isLeader := kv.startOp(*op)
	if isOld {
		reply.Err = ErrIgnored
		if op.OpType == GET {
			reply.Result = kv.data[key2shard(op.Key)].Data[op.Key]
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	if isLeader {
		opMsg, ok := kv.opMp.Load(index)
		if !ok {
			panic(ErrFailed)
		}
		kv.log("%+v wait Index %v/%v", op, kv.lastIndex, index)
		select {
		case result := <-opMsg.(chan Reply):
			kv.log("get Msg:%+v,%+v", op, reply)
			reply.CopyFrom(result)
			if op.SeqId != result.SeqId || op.ClientId != result.ClientId {
				reply.Err = ErrFailed
				kv.log("getFailed:%+v,%+v", op, reply, index)
			}
			return
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) syncConfig(ctx context.Context) {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		kv.mu.RLock()
		needFetchConf := true
		for _, shard := range kv.data {
			if !(shard.Status == ShardStatusRunning || shard.Status == ShardStatusInvalid) {
				needFetchConf = false
				break
			}
		}
		if _, isLeader := kv.rf.GetState(); !isLeader {
			needFetchConf = false
		}
		qConf := kv.config
		kv.mu.RUnlock()
		if needFetchConf {
			conf := kv.sm.Query(qConf.Num + 1)
			if conf.Num == qConf.Num+1 {
				cm := Command{
					CommandType:   ConfigCommand,
					ConfigCommand: conf,
				}
				kv.log("syncConfig%+v", conf)
				kv.mu.Lock()
				kv.startCommand(cm)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *ShardKV) applyCommand(ch raft.ApplyMsg) {
	cm, ok := ch.Command.(Command)

	if ok {
		switch cm.CommandType {
		case LogCommand:
			kv.applyLogCommand(cm.LogCommand.deepCopy(), ch.CommandIndex)
		case ConfigCommand:
			kv.applyConfigCommand(cm.ConfigCommand, ch.CommandIndex)
		case ShardCommand:
			kv.applyShardCommand(cm.ShardCommand.deepCopy(), ch.CommandIndex)
		}
	}
	if ch.CommandIndex != 0 && kv.maxraftstate != -1 && kv.rf.GetStateSize() > kv.maxraftstate {
		kv.log("will create snapshot,%+v", ch)
		by := new(bytes.Buffer)
		e := labgob.NewEncoder(by)
		if e.Encode(kv.data) == nil && e.Encode(kv.reqMp) == nil && e.Encode(kv.config) == nil && e.Encode(kv.lastConfig) == nil {
			kv.rf.Snapshot(ch.CommandIndex, by.Bytes())
		} else {
			panic("create snapshot error")
		}
	}
}

func (kv *ShardKV) applyShardCommand(shardData ShardData, commandIndex int) {
	kv.log("applyShardCommand%+v", shardData)
	kv.data[shardData.ShardKey] = &shardData.Shard
	if kv.data[shardData.ShardKey].Data == nil {
		kv.data[shardData.ShardKey].Data = map[string]interface{}{}
	}
	if v, ok := kv.opMp.Load(commandIndex); ok {
		go func() {
			v.(chan Reply) <- Reply{}
		}()
	}
}

func (kv *ShardKV) applyLogCommand(op Op, commandIndex int) {
	kv.log("applyLogCommand%+v", op)
	if v, ok := kv.reqMp[op.ClientId]; !ok || op.SeqId > v {
		kv.reqMp[op.ClientId] = op.SeqId
		if f, ok := OpNameMap[op.OpType]; ok {
			reply := Reply{
				SeqId:    op.SeqId,
				ClientId: op.ClientId,
				ServerId: strconv.Itoa(kv.me),
				Err:      OK,
			}
			if kv.data[key2shard(op.Key)].Status != ShardStatusRunning {
				reply.Err = ErrWrongLeader
			} else {
				result, err := f(kv, op)
				reply.Result = result
				if err != nil {
					kv.log("reply error%+v", err)
					reply.Err = err.Error()
				}
			}

			if v, ok := kv.opMp.Load(commandIndex); ok {
				go func() {
					kv.log("applier Success!!,%+v,index opt found in opStatus", op)
					v.(chan Reply) <- reply
				}()
			}
		} else {
			panic("ErrOpNotMatch")
		}
	}
}

func (kv *ShardKV) applySnapshot(ch raft.ApplyMsg) {
	kv.log("will applier snapshot,%+v", ch)
	if kv.rf.CondInstallSnapshot(ch.SnapshotTerm, ch.SnapshotIndex, ch.Snapshot) {
		kv.log("applier snapshot,%+v", ch)
		kv.loadSnapshot(ch.Snapshot)
		kv.lastIndex = ch.SnapshotIndex
	}

}

func (kv *ShardKV) applyConfigCommand(config shardctrler.Config, commandIndex int) {
	kv.log("applyConfigCommand%+v,Index%v", config, commandIndex)
	if config.Num <= kv.config.Num {
		//这里可能会由于重启，在回放log的时候导致pullConfig写了版本较小的config，需要屏蔽
		return
	}
	kv.lastConfig = kv.config
	kv.config = config
	for shard, g := range config.Shards {
		_, ok := kv.data[shard]
		if !ok {
			kv.data[shard] = new(Shard)
			kv.data[shard].Data = make(map[string]interface{})
			kv.data[shard].Status = ShardStatusInvalid
		}
		if g == kv.gid {
			if kv.data[shard].Status == ShardStatusInvalid {
				kv.data[shard].Status = ShardStatusPulling
			}
		} else {
			if kv.data[shard].Status == ShardStatusRunning {
				kv.data[shard].Status = ShardStatusErasing
			}
		}
	}
	//if v, ok := kv.opMp.Load(commandIndex); ok {
	//	go func() {
	//		kv.log("applyConfigCommand!!%+v ", config)
	//		v.(chan Reply) <- Reply{}
	//	}()
	//}
}

type PullShardArgs struct {
	ShardKey  int
	Gid       int
	Id        int
	ConfigNum int
}

type GCReply struct {
	Success bool
}

type GCArgs struct {
	ShardKey int
	Gid      int
	Id       int
}

type PullShardReply struct {
	Success bool
	Data    map[string]interface{}
}

func (kv *ShardKV) PullingShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.log("reciv pullshard %+v", args)
	defer kv.log("pullshard reply%+v", reply)

	if args.ConfigNum >= kv.config.Num {
		kv.log("configNum not ready%+v", kv.config)
		reply.Success = false
		return
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.log("not Leader%+v", kv.config)
		reply.Success = false
		return
	}

	if kv.data[args.ShardKey].Status == ShardStatusRunning || kv.data[args.ShardKey].Status == ShardStatusErasing {
		if kv.data[args.ShardKey].Data == nil {
			kv.data[args.ShardKey].Data = make(map[string]interface{})
			kv.log("data is nil ......,but status is %v", kv.data[args.ShardKey].Status)
		}
		reply.Data = kv.data[args.ShardKey].Data
		reply.Success = true
		kv.startCommand(
			Command{
				CommandType: ShardCommand,
				ShardCommand: ShardData{
					Shard:    Shard{Status: ShardStatusInvalid, Data: reply.Data},
					ShardKey: args.ShardKey,
				},
			},
		)
	} else {
		kv.log("failed status not ready%v", kv.data[args.ShardKey].Status)
	}
}

func (kv *ShardKV) Call(servers []string) func(f string, args interface{}, reply *PullShardReply) bool {
	return func(f string, args interface{}, reply *PullShardReply) bool {
		for idx := 0; ; idx = (idx + 1) % len(servers) {
			cl := kv.make_end(servers[idx])
			if ok := cl.Call(f, args, reply); ok && reply.Success {
				return ok
			}
		}
		return false
	}
}

func (kv *ShardKV) pullingShard(ctx context.Context) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		var wg sync.WaitGroup
		kv.mu.RLock()
		for shardKey, _ := range kv.data {
			shardKey := shardKey
			if kv.data[shardKey].Status == ShardStatusPulling {
				wg.Add(1)
				conf := kv.lastConfig
				go pprof.Do(
					ctx, pprof.Labels("path", "/api/pullingShard/call", "kvID", kv.name()),
					func(ctx context.Context) {
						kv.pullShard(
							conf, shardKey,
							func() {
								wg.Done()
							},
						)
					},
				)
			}

		}
		kv.mu.RUnlock()
		wg.Wait()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pullShard(conf shardctrler.Config, shardKey int, callBack func()) {
	defer callBack()
	cm := Command{
		CommandType: ShardCommand,
		ShardCommand: ShardData{
			ShardKey: shardKey,
			Shard:    Shard{Status: ShardStatusRunning, Data: map[string]interface{}{}},
		},
	}
	if conf.Num != 0 {
		gid := conf.Shards[shardKey]
		services := conf.Groups[gid]
		args := PullShardArgs{
			Gid:       kv.gid,
			ShardKey:  shardKey,
			Id:        kv.me,
			ConfigNum: conf.Num,
		}
		reply := PullShardReply{}
		if kv.Call(services)("ShardKV.PullingShard", &args, &reply) {
			cm = Command{
				CommandType: ShardCommand,
				ShardCommand: ShardData{
					ShardKey: shardKey,
					Shard:    Shard{Status: ShardStatusRunning, Data: reply.Data},
				},
			}
			kv.log("pullshard client reply%v", reply)
		}
	}
	kv.mu.Lock()
	index, isLeader := kv.startCommand(cm)
	kv.mu.Unlock()
	if isLeader {
		c, _ := kv.opMp.Load(index)
		select {
		case <-c.(chan Reply):
			kv.log("firstPull fin%v,num%v", shardKey, conf.Num)
		}
	}
}
func (kv *ShardKV) gcing() {
	for !kv.killed() {
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			for i, shard := range kv.data {
				if shard.Status == ShardStatusGCing {
					if kv.lastConfig.Num != 0 {
						gid := kv.lastConfig.Shards[i]
						for _, s := range kv.lastConfig.Groups[gid] {
							args := GCArgs{
								Gid:      kv.gid,
								ShardKey: i,
							}
							reply := GCReply{}
							if ok := kv.make_end(s).Call("ShardKV.GCShard", &args, &reply); ok && reply.Success {
								kv.data[i].Status = ShardStatusRunning
							}
						}
					} else {
						kv.data[i].Status = ShardStatusRunning
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}

}

func (kv *ShardKV) GCShard(args *GCArgs, reply *GCReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.data[args.ShardKey]; !ok {
		reply.Success = false
		return
	}
	if kv.data[args.ShardKey].Status == ShardStatusGCing {
		reply.Success = true
		kv.data[args.ShardKey].Status = ShardStatusInvalid
	}
	return
}
