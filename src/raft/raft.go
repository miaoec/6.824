package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"context"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"

	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

func (s State) String() string {
	switch s {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return "Unk"
}

const (
	Follower State = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        State
	term         int
	voteFor      int
	electionTime *time.Ticker

	//logs  []LogEntry
	logs2 []LogEntry
	//snapshot:[0:lastIncludeEntry.Index),logs:[lastIncludeEntry.Index:++]
	//lastIncludeEntry.Index int
	//lastIncludeEntry.Term     int
	lastIncludeEntry LogEntry
	commitIndex      int
	lastApplied      int

	nextIndex  []int
	matchIndex []int

	applyChan chan ApplyMsg

	tracerName string
	LogName    string
}

var IsDebug = false

func (rf *Raft) GetStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) indexLog(index int) LogEntry {
	if index < rf.lastIncludeEntry.Index {
		panic("index(" + strconv.Itoa(index) + ")<rf.lastIncludeEntry.Index")
	}
	if index == rf.lastIncludeEntry.Index {
		//rf.lastIncludeEntry.Index =
		return rf.lastIncludeEntry
	}
	return rf.logs2[index-rf.lastIncludeEntry.Index-1]
}

//[l,r)
func (rf *Raft) rangeLog(l, r int) []LogEntry {
	if r == -1 {
		r = len(rf.logs2) + rf.lastIncludeEntry.Index + 1
	}
	l = max(l, rf.lastIncludeEntry.Index+1)
	if l <= rf.lastIncludeEntry.Index {
		panic("range(" + strconv.Itoa(l) + "," + strconv.Itoa(r) + ")<rf.lastIncludeEntry.Term")
	}
	return rf.logs2[l-rf.lastIncludeEntry.Index-1 : r-rf.lastIncludeEntry.Index-1]

}

func (rf *Raft) lastLog() LogEntry {
	//return
	//return LogEntry{}
	if len(rf.logs2) != 0 {
		return rf.logs2[len(rf.logs2)-1]
	}
	return LogEntry{Term: rf.lastIncludeEntry.Term}
}

func (rf *Raft) lastIndex() int {
	return rf.lastIncludeEntry.Index + len(rf.logs2)
}

func (rf *Raft) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"state":   rf.state,
		"term":    rf.term,
		"logs":    rf.logs2,
		"voteFor": rf.voteFor,
	}
}

type LogEntry struct {
	Cmd   interface{}
	Index int
	Term  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.state == Leader
	return term, isleader
}

func (rf *Raft) becomeLeader() {
	_, _, l, _ := runtime.Caller(1)
	rf.log("%v,becomeLeader", l)
	rf.state = Leader
	//noop log
	//加上nooop log 会导致TestBasicAgree2B与TestRPCBytes2B 错误,破案了，noop日志在service端跳过没unlock导致死锁。。。。
	rf.logs2 = append(rf.logs2, LogEntry{Cmd: 0, Term: rf.term, Index: rf.lastIndex() + 1})
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.lastIndex() + 1
		rf.matchIndex[i] = -1
	}

}

func (rf *Raft) startElection(ctx context.Context) {

	rf.term++
	rf.state = Candidate
	startTerm := rf.term
	rf.voteFor = rf.me
	voteCount := 1

	rf.persist()
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         startTerm,
			Id:           rf.me,
			LastLogTerm:  rf.lastLog().Term,
			LastLogIndex: rf.lastIndex(),
		}
		reply := &RequestVoteReply{}

		go func(server int) {
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.term == startTerm && reply.Grant { //避免之前的投票导致重新当回leader
					voteCount++
					if rf.state == Candidate {
						if voteCount*2 >= len(rf.peers)+1 {
							rf.becomeLeader()
							return
						}
					}
				}
				if reply.Term > startTerm {
					rf.becomeFollower(reply.Term)
					return
				}
			} else {
				rf.log("failed sendRequestVote to%v", server)
			}
		}(i)
	}
}

func (rf *Raft) sendHeartBeat() {
	for {
		time.Sleep(time.Duration(100) * time.Millisecond)
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		rf.sendOne()
		//opentracing
		//	不能够协程去调用，可能会导致出现Follower发心跳

	}
}

func (rf *Raft) sendOne() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	rf.matchIndex[rf.me] = rf.lastIndex()
	for i, _ := range rf.peers {
		if i == rf.me {
			rf.updateCommitIndex()
			continue
		}
		var entries []LogEntry
		preLog := LogEntry{Term: -1}
		if rf.nextIndex[i]-1 < rf.lastIncludeEntry.Index && rf.lastIncludeEntry.Term != -1 {
			installArgs := InstallSnapshotArgs{
				Term:             rf.term,
				LastIncludeTerm:  rf.lastIncludeEntry.Term,
				LastIncludeIndex: rf.lastIncludeEntry.Index,
				SnapShot:         rf.persister.ReadSnapshot(),
			}
			reply := InstallSnapshotReply{}
			rf.log(
				"send InstallSnapshot(%+v): installArgs:%+v", i, installArgs.logData(),
			)
			go func(server int) {
				if rf.sendInstallSnapshot(server, &installArgs, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					//要判断leader
					if reply.Term > rf.term {
						rf.becomeFollower(reply.Term)
						return
					}
					if rf.state != Leader {
						return
					}
					if reply.Success {
						rf.matchIndex[server] = installArgs.LastIncludeIndex
						rf.nextIndex[server] = min(rf.matchIndex[server]+1, rf.lastIndex()+1)
						rf.updateCommitIndex()
					} else {
						//这里需要处理失败的情况，follower已经同步了快照，将nextIndex重置为rf.lastIndex+1,否则会出现一直不失败的情况
						//todo:也许rf.nextIndex[server]++更合适？
						rf.log("reply,faild sendInstallSnapshot to%v", server)
						rf.nextIndex[server] = rf.lastIndex() + 1
						//rf.nextIndex[server] = max(reply.ConflictIndex, 0)
						//if rf.nextIndex[server] >= 1 {
						//	rf.nextIndex[server]--
						//}
					}
				} else {
					rf.log("faild sendInstallSnapshot to%v", server)
				}
			}(i)
			rf.nextIndex[i]++
			continue
		}
		if rf.nextIndex[i] <= rf.lastIndex() {
			entries = rf.rangeLog(rf.nextIndex[i], -1)
		}
		if rf.nextIndex[i] >= 1 {
			preLog = rf.indexLog(rf.nextIndex[i] - 1)
		}
		rf.log("send entries(%v): nextIndex:%v,matchIndex:%v", i, rf.nextIndex[i], rf.matchIndex[i])
		args := &AppendEntriesArgs{
			Term:        rf.term,
			Id:          rf.me,
			CommitIndex: rf.commitIndex,
			PreLogIndex: rf.nextIndex[i] - 1,
			PreLogTerm:  preLog.Term,
			Entries:     entries,
			Context:     nil,
		}
		reply := &AppendEntriesReply{}
		go func(server int) {
			ok := rf.sendAppendEntries(server, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//要判断leader

				if reply.Term > rf.term {
					rf.becomeFollower(reply.Term)
					return
				}
				if rf.state != Leader {
					return
				}
				if reply.Success {
					rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
					rf.nextIndex[server] = min(rf.matchIndex[server]+1, rf.lastIndex()+1)
					rf.updateCommitIndex()
				} else {
					rf.nextIndex[server] = max(reply.ConflictIndex, 0)
					//if rf.nextIndex[server] >= 1 {
					//	rf.nextIndex[server]--
					//}
				}
			} else {
				rf.log("faild send entries to%v", server)
			}

		}(i)
	}

}

//leader 不能够主动提交之前任期的日志
func (rf *Raft) updateCommitIndex() {
	idx := rf.getMajorityMatchIndex()
	// && rf.lastIncludeEntry.Index > idx && rf.indexLog(idx).Term == rf.term
	if idx >= 0 {
		rf.commitIndex = idx
	}
}

//func (rf *Raft) getMajorityIndex() int {
//	tmp := make([]int, len(rf.matchIndex))
//	copy(tmp, rf.matchIndex)
//	tmp[rf.me] = len(rf.logs)
//	sort.Ints(tmp)
//	return tmp[len(tmp)/2]
//}
func (rf *Raft) getMajorityMatchIndex() int {
	tmp := make([]int, len(rf.matchIndex))
	copy(tmp, rf.matchIndex)

	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))

	idx := len(tmp) / 2
	return tmp[idx]

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	rf.persister.SaveRaftState(rf.getState())
}

func (rf *Raft) getState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.lastIncludeEntry.Term) != nil || e.Encode(rf.lastIncludeEntry.Index) != nil ||
		e.Encode(rf.logs2) != nil ||
		e.Encode(rf.voteFor) != nil ||
		e.Encode(rf.term) != nil {
		log.Fatal("encode Error")
	}
	return w.Bytes()
}

//
// restore previously persisted state.
//

func (rf *Raft) readPersist(data []byte) {
	rf.log("readPersist")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	term := 0
	voteFor := 0
	logs := []LogEntry{}
	lastIncludeIndex := -1
	lastIncludeLogTerm := -1
	if err := d.Decode(&lastIncludeLogTerm); err != nil {
		log.Fatalf("readPersist Error,%+v", err)
	}
	if err := d.Decode(&lastIncludeIndex); err != nil {
		log.Fatalf("readPersist Error,%+v", err)
	}
	if err := d.Decode(&logs); err != nil {
		log.Fatalf("readPersist Error,%+v", err)
	}
	if err := d.Decode(&voteFor); err != nil {
		log.Fatalf("readPersist Error,%+v", err)
	}
	if err := d.Decode(&term); err != nil {
		log.Fatalf("readPersist Error,%+v", err)
	}
	rf.term = term
	rf.voteFor = voteFor
	rf.logs2 = logs
	rf.lastIncludeEntry.Index = lastIncludeIndex
	rf.lastIncludeEntry.Term = lastIncludeLogTerm
	rf.commitIndex = rf.lastIncludeEntry.Index
	rf.lastApplied = rf.lastIncludeEntry.Index //读完状态后lastApplied初始化为lastIncludeIndex
	rf.log("readPersist %+v", rf.logs2)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIncludedIndex -= 1
	defer rf.log("snapshot index%v", lastIncludedIndex)
	if rf.commitIndex >= lastIncludedIndex {
		return false
	}
	if rf.lastIncludeEntry.Index >= lastIncludedIndex {
		return false
	}
	if rf.lastIndex() < lastIncludedIndex || len(rf.logs2) == 0 {
		rf.logs2 = []LogEntry{}
	} else {
		rf.logs2 = rf.logs2[lastIncludedIndex-rf.lastIncludeEntry.Index:]
	}
	rf.persister.SaveStateAndSnapshot(rf.getState(), snapshot)
	rf.lastIncludeEntry.Term = lastIncludedTerm
	rf.lastIncludeEntry.Index = lastIncludedIndex
	rf.commitIndex = max(rf.commitIndex, lastIncludedIndex)
	rf.lastApplied = max(rf.commitIndex, lastIncludedIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//sshot
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index -= 1
	defer rf.log("snapshot index%v", index)
	if rf.commitIndex < index {
		return
	}
	if rf.lastIncludeEntry.Index >= index {
		return
	}
	rf.lastIncludeEntry.Term = rf.indexLog(index).Term
	rf.logs2 = rf.logs2[index-rf.lastIncludeEntry.Index:]
	rf.lastIncludeEntry.Index = index
	rf.persister.SaveStateAndSnapshot(rf.getState(), snapshot)
	//rf.commitIndex = index
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Context      []byte
	Term         int
	Id           int
	LastLogIndex int
	LastLogTerm  int
}

func (a RequestVoteArgs) LogData() interface{} {
	a.Context = nil
	return a
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Grant bool
	Term  int
	Msg   string
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) becomeFollower(term int) {
	_, _, l, _ := runtime.Caller(1)
	defer rf.persist()
	rf.log("%v,becomeFollower:%v", l, term)
	rf.state = Follower
	if term > rf.term {
		//只有term变更的时候才能把votefor重置
		rf.voteFor = -1
	}
	rf.term = term

}

const RequestVoteMsgAlreadyVoteFor = " already vote for on"

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer rf.log("req:%+v,rsp%+v,voteFor:%+v", args.LogData(), reply, rf.voteFor)
	reply.Term = rf.term
	reply.Grant = false
	if rf.term < args.Term {
		rf.becomeFollower(args.Term)
	}

	if rf.voteFor != -1 {
		reply.Msg = RequestVoteMsgAlreadyVoteFor + strconv.Itoa(rf.term)
		return
	}
	//mark: 这里比较candidate的日志算法比较新的逻辑是关键，先比较最后一条日志的任期，任期大的日志新，如果任期相同比较index，index长的日志新
	if rf.term <= args.Term && (rf.voteFor == -1 || rf.voteFor == args.Id) &&
		(rf.lastLog().Term < args.LastLogTerm ||
			rf.lastLog().Term == args.LastLogTerm && rf.lastIndex() <= args.LastLogIndex) {
		reply.Term = rf.term
		reply.Grant = true
		rf.voteFor = args.Id
		rf.electionTime.Reset(rf.newRandTimeOut())
	}

}

//func (rf *Raft) lastLog() LogEntry {
//	if len(rf.logs) != 0 {
//		return rf.logs[len(rf.logs)-1]
//	}
//	return LogEntry{Term: -1}
//}

type AppendEntriesArgs struct {
	Context     []byte
	Term        int
	Id          int
	Entries     []LogEntry
	PreLogIndex int
	PreLogTerm  int
	CommitIndex int
}

func (a AppendEntriesArgs) LogData() interface{} {
	a.Context = nil
	var tmp []LogEntry
	if len(a.Entries) > 6 {
		tmp = append(tmp, a.Entries[:2]...)
		tmp = append(tmp, LogEntry{Cmd: 12312312345, Term: -1})
		tmp = append(tmp, a.Entries[len(a.Entries)-5:]...)
		a.Entries = tmp
	}

	return a
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
	Msg           string
}

const AppendEntriesTermTooOld = "term too old "
const AppendEntriesPreLogTConflict = "preLog term conflict "

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//rf.tracer.StartSpan()

	reply.Term = rf.term
	rf.electionTime.Reset(rf.newRandTimeOut())
	defer rf.log("heartBeatRecv req:%+v,rsp:%+v", args.LogData(), reply)
	if args.Term < rf.term {
		rf.state = Candidate
		reply.Success = false
		reply.Msg = AppendEntriesTermTooOld
		return
	} else if args.Term > rf.term { //要判断term
		rf.becomeFollower(args.Term)
	} else if rf.state != Follower {
		rf.state = Follower
	}
	//if args.PreLogIndex <=rf.lastIncludeEntry.Index{
	//	reply.Success = false
	//	reply.Msg = AppendEntriesPreLogTConflict
	//	reply.ConflictIndex = rf.
	//	return
	//}
	if args.PreLogIndex < rf.lastIncludeEntry.Index || args.PreLogIndex > rf.lastIndex() {
		reply.Success = false
		reply.Msg = AppendEntriesPreLogTConflict
		reply.ConflictIndex = rf.lastIndex() + 1
		return
	}
	if args.PreLogIndex != -1 && args.PreLogTerm != rf.indexLog(args.PreLogIndex).Term {
		reply.Success = false
		reply.Msg = AppendEntriesPreLogTConflict
		if args.PreLogIndex == 0 {
			reply.ConflictIndex = 0
		} else {
			reply.ConflictIndex = args.PreLogIndex //极端情况出现[...,commitIndex,preLogIndex]，可能找不到，兜底策略向前移动一位
			for i := args.PreLogIndex - 1; i > rf.commitIndex; i-- {
				if i == 0 || rf.indexLog(i).Term != rf.indexLog(args.PreLogIndex).Term {
					reply.ConflictIndex = i
					break
				}
			}
		}
		return
	}
	reply.Success = true
	//可能收到延时的重复日志附加请求而导致日志不必要的截断，从而导致已经提交的日志丢失。
	//rf.logs = rf.logs[:args.PreLogIndex+1]
	//rf.logs = append(rf.logs, args.Entries...)
	if args.PreLogIndex == -1 && len(rf.logs2) == 0 {
		rf.logs2 = append(rf.logs2, args.Entries...)
	} else {
		for i, entry := range args.Entries {
			if args.PreLogIndex+1+i > rf.lastIndex() || rf.indexLog(args.PreLogIndex+1+i).Term != entry.Term {
				rf.logs2 = rf.rangeLog(0, args.PreLogIndex+1+i) //修复bug
				rf.logs2 = append(rf.logs2, args.Entries[i:]...)
				break
			}
		}
	}

	if args.CommitIndex > rf.commitIndex {

		//在某些网络不好的情况下，可能会导致心跳乱序，出现commitIndex更新 但是log还没有到位的情况，所以需要min一下
		rf.commitIndex = min(args.CommitIndex, rf.lastIndex())
	}
}

type InstallSnapshotArgs struct {
	Term             int
	LastIncludeIndex int
	LastIncludeTerm  int
	SnapShot         []byte
}

func (r InstallSnapshotArgs) logData() InstallSnapshotArgs {
	r.SnapShot = nil
	return r
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//defer rf.persister
	rf.log("InstallSnapshot Recv:args%+v", args.logData())
	reply.Term = rf.term
	rf.electionTime.Reset(rf.newRandTimeOut())
	if args.Term < rf.term {
		reply.Success = false
		return
	}
	if args.Term > rf.term {
		rf.becomeFollower(args.Term)
	}
	if args.LastIncludeIndex <= rf.lastIncludeEntry.Index {
		return
	}
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.SnapShot,
		SnapshotIndex: args.LastIncludeIndex + 1,
		SnapshotTerm:  args.LastIncludeTerm,
	}
	rf.log(
		"apply Snap%+v,", ApplyMsg{
			SnapshotValid: true,
			//Snapshot:      args.SnapShot,
			SnapshotIndex: args.LastIncludeIndex + 1,
			SnapshotTerm:  args.LastIncludeTerm,
		},
	)
	//这里不加有可能造成死锁
	go func() { rf.applyChan <- applyMsg }()
	reply.Success = true
}

//!!! 写错了
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == Leader
	if rf.state == Leader {
		rf.log("append:%+v", LogEntry{Cmd: command, Term: rf.term})
		rf.logs2 = append(rf.logs2, LogEntry{Cmd: command, Term: rf.term, Index: rf.lastIndex() + 1})
		term = rf.term
		rf.persist()
		index = rf.lastIndex() + 1
		go rf.sendOne()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//
	//rf.log("crash,save logs%+v", rf.logs2)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	//rf.mainSpan.Finish()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) End() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	time.Sleep(1 * time.Second)
}

// The  go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) logTrac(str string, args ...interface{}) {

}

func (rf *Raft) log(str string, args ...interface{}) {
	if rf.LogName == "" {
		rf.mu.Lock()
		rf.LogName = strconv.Itoa(rf.me)
		rf.mu.Unlock()
	}
	if IsDebug {
		log.Printf(
			"%s,%+v\n", fmt.Sprintf(
				fmt.Sprintf(
					"raft(id:%v,term:%v,state:%v,votefor:%v,lastIndex%v,commitIndex:%v,lastApplied%v)##: %v",
					rf.LogName,
					rf.term, rf.state,
					rf.voteFor, rf.lastIncludeEntry.Index, rf.commitIndex, rf.lastApplied,
					str,
				),
				args...,
			), rf.logs2,
		)
	}

}

func (rf *Raft) last10Log() []LogEntry {
	var tmp []LogEntry
	if len(rf.logs2) > 6 {
		tmp = append(tmp, rf.logs2[:2]...)
		tmp = append(tmp, LogEntry{Cmd: 12312312345, Term: -1})
		tmp = append(tmp, rf.logs2[len(rf.logs2)-5:]...)
	} else {
		return rf.logs2
	}
	return tmp
}

func (rf *Raft) newRandTimeOut() time.Duration {
	return time.Duration(rand.Int()%200+200) * time.Millisecond
}
func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.electionTime.C:
			rf.mu.Lock()
			ctx := context.Background()
			// rf.log("ticker term%v", rf.state)
			if rf.state == Leader {
				rf.mu.Unlock()
				continue
			}
			if rf.killed() == false {
				rf.log(" electionTimeout startElection")
				//这里务必要重置时间，可能恰好大家时间都差不多，每个节点一直投自己，陷入选举死循环，实测两个节点概率超过百分之2，与随机函数相关
				rf.electionTime.Reset(rf.newRandTimeOut())
				rf.startElection(ctx)
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		time.Sleep(time.Duration(10) * time.Millisecond)
		rf.mu.Lock()
		var msgs []ApplyMsg
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msgs = append(
					msgs, ApplyMsg{
						CommandValid: true,
						Command:      rf.indexLog(i).Cmd,
						CommandIndex: i + 1,
					},
				)
				rf.log(
					"addMsg log%+v", ApplyMsg{
						CommandValid: true,
						Command:      rf.indexLog(i).Cmd,
						CommandIndex: i + 1,
					},
				)
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		for i, _ := range msgs {
			rf.applyChan <- msgs[i]
			rf.log(
				"apply log%+v", msgs[i],
			)
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(
	peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	//log.SetFlags(log.Lshortfile | log.Ldate | log.Lmicroseconds)
	//f, err := os.OpenFile("batch_test_tmp/raft-"+strconv.Itoa(me)+".log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	//log.SetOutput(f)
	//if err != nil {
	//	panic(err)
	//}
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.term = 1
	rf.state = Candidate
	rf.voteFor = -1
	rf.electionTime = time.NewTicker(rf.newRandTimeOut())
	rf.applyChan = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.lastIncludeEntry.Index = -1
	rf.lastIncludeEntry.Term = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	// initialize from state persisted before a crash

	rf.tracerName = "raft-" + strconv.Itoa(os.Getppid()) + "-" + strconv.Itoa(me)

	//tracer, _ := tracing.Init(rf.tracerName)

	rf.readPersist(persister.ReadRaftState())
	//rf.mainSpan.SetTag("pGroup", strconv.Itoa(os.Getppid()))
	//ctx := opentracing.ContextWithSpan(context.Background(), rf.mainSpan)
	//tracer.StartSpan("s")

	go rf.ticker()
	go rf.sendHeartBeat()
	go rf.applyLoop()
	return rf
}
