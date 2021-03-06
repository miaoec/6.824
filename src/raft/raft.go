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
	"math/rand"
	"runtime"
	"sort"
	"strconv"

	//"crypto/rand"
	//	"bytes"
	"sync"
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

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        int
	term         int
	voteFor      int
	electionTime *time.Ticker

	logs        []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyChan chan ApplyMsg
}

type LogEntry struct {
	Cmd  interface{}
	Term int
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
	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = -1
	}
}

func (rf *Raft) startElection() {
	rf.term++
	rf.state = Candidate
	startTerm := rf.term
	rf.voteFor = rf.me
	voteCount := 1

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         startTerm,
			Id:           rf.me,
			LastLogTerm:  rf.lastLog().Term,
			LastLogIndex: len(rf.logs) - 1,
		}
		reply := &RequestVoteReply{}
		go func(server int) {
			rf.sendRequestVote(server, args, reply)
			if reply.Grant {
				rf.mu.Lock()

				voteCount++
				if voteCount*2 >= len(rf.peers)+1 {
					rf.log("to leader")
					rf.becomeLeader()
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.term {
					rf.voteFor = -1
					rf.log("to Follower")
					rf.becomeLeader()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

			}
		}(i)
	}
	//go rf.ticker()

}

func (rf *Raft) sendHeartBeat() {
	for {
		time.Sleep(time.Duration(50) * time.Millisecond)
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}

		go func() {
			for i, _ := range rf.peers {
				rf.mu.Lock()
				if i == rf.me {
					rf.matchIndex[i] = len(rf.logs) - 1
					idx := rf.getMajorityMatchIndex()
					rf.commitIndex = idx
					rf.mu.Unlock()
					continue
				}
				var entries []LogEntry
				preLog := LogEntry{Term: -1}

				if rf.nextIndex[i] < len(rf.logs) {
					entries = rf.logs[rf.nextIndex[i]:]
				}
				if rf.nextIndex[i] >= 1 {
					preLog = rf.logs[rf.nextIndex[i]-1]
				}
				rf.log("send entries(%v): nextIndex:%v,matchIndex:%v", i, rf.nextIndex[i], rf.matchIndex[i])
				args := &AppendEntriesArgs{
					Term:        rf.term,
					Id:          rf.me,
					CommitIndex: rf.commitIndex,
					PreLogIndex: rf.nextIndex[i] - 1,
					PreLogTerm:  preLog.Term,
					Entries:     entries,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()
				go func(server int) {
					rf.sendAppendEntries(server, args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.term {
						rf.becomeFollower(reply.Term)
						return
					}
					if reply.Success {
						rf.matchIndex[server] = args.PreLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
						idx := rf.getMajorityMatchIndex()
						rf.commitIndex = idx
					} else {
						if rf.nextIndex[server] > 1 {
							rf.nextIndex[server]--
						}
					}
				}(i)
			}
		}()
		rf.mu.Unlock()
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	Id           int
	LastLogIndex int
	LastLogTerm  int
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
	rf.log("%v,becomeFollower:%v", l, term)
	rf.state = Follower
	rf.term = term
	rf.voteFor = -1
}

const RequestVoteMsgAlreadyVoteFor = " already vote for on"

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.term
	reply.Grant = false
	if rf.term < args.Term {
		rf.becomeFollower(args.Term)
	}
	//mark: ????????????candidate????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????index???index???????????????
	if rf.term <= args.Term && rf.voteFor == -1 &&
		(rf.lastLog().Term < args.LastLogTerm ||
			rf.lastLog().Term == args.LastLogTerm && len(rf.logs) <= args.LastLogIndex+1) {
		reply.Term = rf.term
		reply.Grant = true
		rf.electionTime.Reset(rf.newRandTimeOut())
	}
	if rf.voteFor != -1 {
		reply.Msg = RequestVoteMsgAlreadyVoteFor + strconv.Itoa(rf.term)
	}

	defer rf.log("req:%+v,rsp%+v,voteFor:%+v", *args, *reply, rf.voteFor)
}

func (rf *Raft) lastLog() LogEntry {
	if len(rf.logs) != 0 {
		return rf.logs[len(rf.logs)-1]
	}
	return LogEntry{Term: -1}
}

type AppendEntriesArgs struct {
	Term        int
	Id          int
	Entries     []LogEntry
	PreLogIndex int
	PreLogTerm  int
	CommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Msg     string
}

const AppendEntriesTermTooOld = "term too old "
const AppendEntriesPreLogTConflict = "preLog term conflict "

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	rf.electionTime.Reset(rf.newRandTimeOut())
	defer rf.log("heartBeatRecv req:%+v,rsp:%+v", args, reply)
	if args.Term < rf.term {
		rf.state = Candidate
		reply.Success = false
		reply.Msg = AppendEntriesTermTooOld
		return
	}
	if args.PreLogIndex >= len(rf.logs) || args.PreLogIndex != -1 && args.PreLogIndex < len(rf.logs) && args.PreLogTerm != rf.logs[args.PreLogIndex].Term {
		reply.Success = false
		reply.Msg = AppendEntriesPreLogTConflict
		return
	}
	reply.Success = true
	//j := 0
	//b := *args
	//c := make([]LogEntry, len(rf.logs))
	//copy(c, rf.logs)
	//for i := b.PreLogIndex + 1; i < len(rf.logs) && j < len(args.Entries); i++ {
	//	rf.logs[i] = args.Entries[j]
	//	j++
	//}
	rf.logs = rf.logs[:args.PreLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)

	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.CommitIndex, len(rf.logs)-1)
	}

}
func min(a, b int) int {
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
		rf.logs = append(rf.logs, LogEntry{Cmd: command, Term: rf.term})
		index = len(rf.logs)
		term = rf.term
	}

	// Your code here (2B).

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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The  go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) log(str string, args ...interface{}) {
	//fmt.Printf(fmt.Sprintf("raft(id:%v,term:%v,state:%v)##: %v\n", rf.me, rf.term, rf.state, str), args...)
}

func (rf *Raft) newRandTimeOut() time.Duration {
	return time.Duration(rand.Int()%150+150) * time.Millisecond
}
func (rf *Raft) ticker() {

	for {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//if startTerm != rf.term {
		//	rf.mu.Unlock()
		//	continue
		//}
		select {
		case <-rf.electionTime.C:
			rf.mu.Lock()
			// rf.log("ticker term%v", rf.state)
			if rf.state == Leader {
				rf.mu.Unlock()
				continue
			}
			if rf.killed() == false {
				rf.startElection()
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) applyLoop() {
	for {
		time.Sleep(time.Duration(10) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.applyChan <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Cmd,
					CommandIndex: i + 1,
				}
				rf.log("apply log%+v", ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[i].Cmd,
					CommandIndex: i + 1,
				})
			}
		}

		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.term = 1
	rf.state = Follower
	rf.voteFor = -1
	rf.electionTime = time.NewTicker(rf.newRandTimeOut())
	rf.applyChan = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = -1
	rf.lastApplied = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartBeat()
	go rf.applyLoop()
	return rf
}
