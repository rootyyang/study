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
	//	"bytes"

	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type raftState int32

const (
	follower = iota
	candidate
	leader
)

type syncRetryNotify struct {
	index        int
	termNotMatch bool
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	clusterMajority int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A
	currentTerm int
	voteFor     int
	state       raftState

	//control request vote time
	tryRequestVoteTimeMutex sync.RWMutex
	tryRequestVoteTime      time.Time

	//control stop goroutine
	cancelFunc    context.CancelFunc
	cancelContext context.Context

	//2B
	logs                  raftLogs
	nextIndex             []int
	matchIndex            []int
	peersSyncRetryChannel []chan syncRetryNotify
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// TODO Entries 应该包含term

type AppendEntiresArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []OneRaftLog
	LeaderCommit int
}
type AppendEntiresReply struct {
	Term    int
	Success bool
}

// TODO 解决乱序问题，如果在insert的时候，返回ErrIndexGreaterThanMax，可以交给一个队列
func (rf *Raft) AppendEntires(args *AppendEntiresArgs, reply *AppendEntiresReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) != 0 {
		DPrintf("node[%v]  Term[%v] Commit[%v] get appendEntires from %+v", rf.me, rf.currentTerm, rf.logs.getCommitIndex(), *args)
	}
	if args.LeaderTerm < rf.currentTerm {
		*reply = AppendEntiresReply{Term: rf.currentTerm, Success: false}
		return
	}
	err := rf.logs.check(args.PrevLogTerm, args.PrevLogIndex)
	if err != nil {
		if err == ErrParamError {
			DPrintf("Fatel node[%v] Term[%v] Commit[%v] get appendEntires from %+v", rf.me, rf.currentTerm, rf.logs.getCommitIndex(), *args)
			//panic("this")
		}
		*reply = AppendEntiresReply{Term: rf.currentTerm, Success: false}
		return
	}
	rf.currentTerm = args.LeaderTerm
	rf.state = follower
	rf.voteFor = -1
	rf.setNextRetryVote()
	rf.logs.insert(args.PrevLogTerm, args.PrevLogIndex, args.Entries)
	rf.logs.commit(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	if len(args.Entries) != 0 {
		DPrintf("node[%v]  Term[%v] Commit[%v] appendEntires reply true", rf.me, rf.currentTerm, rf.logs.commitIndex)
	}
	*reply = AppendEntiresReply{Term: rf.currentTerm, Success: true}
	return
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("node[%v]  Term[%v] get request vote from node[%v] term[%v]", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	isToDataLastLog := rf.logs.upToDateLast(args.LastLogTerm, args.LastLogIndex)
	if args.Term < rf.currentTerm {
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	} else if args.Term == rf.currentTerm {
		if rf.state == follower && (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && isToDataLastLog {
			rf.voteFor = args.CandidateId
			//刷新时钟
			rf.setNextRetryVote()
			DPrintf("node[%v] request true", rf.me)
			*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
			return
		}
		DPrintf("node[%v] request false", rf.me)
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
		return
	}
	rf.currentTerm = args.Term
	rf.state = follower
	rf.setNextRetryVote()
	if isToDataLastLog {
		rf.voteFor = args.CandidateId
		DPrintf("node[%v] request true", rf.me)
		*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
		return
	}
	rf.voteFor = -1
	*reply = RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	return
}

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
// handler function on the server side does not returf.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
/*
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}*/

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
func (rf *Raft) Start(command interface{}) (rIndex int, rTerm int, rIsLeader bool) {
	//DPrintf("node[%v]  Term[%v] begin start", rf.me, rf.currentTerm)
	rIndex = -1
	rTerm = -1
	rIsLeader = true

	// Your code here (2B).
	rf.mu.Lock()
	if rf.state != leader {
		rIsLeader = false
		rf.mu.Unlock()
		return
	}
	oneLog := OneRaftLog{Term: rf.currentTerm, Command: command}
	msgIndex, prevIndex, prevTerm, err := rf.logs.append(oneLog)
	if err != nil {
		rf.mu.Unlock()
		return
	}
	rf.nextIndex[rf.me] = msgIndex + 1
	rf.matchIndex[rf.me] = msgIndex

	rIndex = msgIndex
	rTerm = rf.currentTerm

	appendEntiresArgs := &AppendEntiresArgs{LeaderTerm: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, LeaderCommit: rf.logs.getCommitIndex(), Entries: []OneRaftLog{oneLog}}
	DPrintf("node[%v]  Term[%v] Commit[%v] Start command index[%v] command[%v]", rf.me, rf.currentTerm, rf.logs.getCommitIndex(), msgIndex, command)
	rf.mu.Unlock()
	//同步数据
	var successNum int32 = 1
	for key, value := range rf.peers {
		//赋值给临时变量，否则，可能协程中引用同一个变量
		if key == rf.me {
			continue
		}
		onePeerKey := key
		onePeer := value
		go func() {
			var reply AppendEntiresReply
			if ok := onePeer.Call("Raft.AppendEntires", appendEntiresArgs, &reply); !ok {
				//通知重试
				rf.peersSyncRetryChannel[onePeerKey] <- syncRetryNotify{index: msgIndex, termNotMatch: false}
				return
			}
			//如果发现一个Term比当前节点大，直接变为follower就可以，这里不会跟commit的逻辑冲突，直接认为自己失败
			if reply.Term > oneLog.Term {
				//更新节点为follower
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = follower
					rf.voteFor = -1
					rf.setNextRetryVote()
				}
				rf.mu.Unlock()
				return
			}
			if !reply.Success {
				rf.peersSyncRetryChannel[onePeerKey] <- syncRetryNotify{index: msgIndex, termNotMatch: true}
				return
			}
			newSuccess := atomic.AddInt32(&successNum, 1)
			if newSuccess == int32(rf.clusterMajority) {
				rf.mu.Lock()
				//TODO 是否存在 commit的时候，对应消息，已经commit过了
				rf.logs.commit(msgIndex, msgIndex)
				rf.mu.Unlock()
			}
			//更新nextIndex和matchIndex
			rf.mu.Lock()
			if rf.nextIndex[onePeerKey] <= msgIndex {
				rf.nextIndex[onePeerKey] = msgIndex + 1
			}
			if rf.matchIndex[onePeerKey] < msgIndex {
				rf.matchIndex[onePeerKey] = msgIndex
			}
			DPrintf("node[%v]  Term[%v] Commit[%v] in start handle reply from node[%v] args[%+v] reply[%+v]", rf.me, rf.currentTerm, rf.logs.getCommitIndex(), onePeerKey, *appendEntiresArgs, reply)
			rf.mu.Unlock()
		}()
	}
	return
}

func (rf *Raft) tickSyncLog(pNodeKey int) {
	for {
		select {
		case <-rf.cancelContext.Done():
			return
		case notify := <-rf.peersSyncRetryChannel[pNodeKey]:
			rf.mu.Lock()
			if rf.state != leader {
				rf.mu.Unlock()
				break
			}
			matchIndex := rf.matchIndex[pNodeKey]
			if notify.index < matchIndex {
				//在之前的重试中，已经完成了同步
				rf.mu.Unlock()
				break
			}
			if notify.index < rf.nextIndex[pNodeKey] {
				//更新nextIndex
				nextTryIndex := notify.index
				if notify.termNotMatch {
					nextTryIndex, err := rf.logs.getNextTryWhenAppendEntiresFalse(notify.index)
					if err != nil || nextTryIndex <= matchIndex {
						nextTryIndex = matchIndex + 1
					}
				}
				rf.nextIndex[pNodeKey] = nextTryIndex
			}
			rf.mu.Unlock()
			for {
				rf.mu.Lock()
				if rf.nextIndex[pNodeKey] > notify.index {
					rf.mu.Unlock()
					break
				}
				nextIndex := rf.nextIndex[pNodeKey]
				sub := notify.index - nextIndex + 1
				oneSyncNum := 20
				if sub < 20 {
					oneSyncNum = sub
				}
				prevTerm, logs, afterEndIndex, _ := rf.logs.get(nextIndex, oneSyncNum)
				leaderCommitIndex := rf.logs.getCommitIndex()
				appendEntiresArgs := &AppendEntiresArgs{LeaderTerm: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: nextIndex - 1, PrevLogTerm: prevTerm, LeaderCommit: leaderCommitIndex, Entries: logs}
				rf.mu.Unlock()
				var reply AppendEntiresReply
				if ok := rf.peers[pNodeKey].Call("Raft.AppendEntires", appendEntiresArgs, &reply); !ok {
					time.Sleep(time.Duration(5 * time.Millisecond))
					continue
				}
				if !reply.Success {
					// 更新nextIndex进行retry
					// 不可能超出
					// TODO 判断如果小于，则同步快照
					rf.mu.Lock()
					nextTryIndex, _ := rf.logs.getNextTryWhenAppendEntiresFalse(nextIndex)
					rf.nextIndex[pNodeKey] = nextTryIndex
					rf.mu.Unlock()
					//判断有问题
					continue
				}
				rf.mu.Lock()
				//给你更新next和match
				rf.nextIndex[pNodeKey] = afterEndIndex
				rf.matchIndex[pNodeKey] = afterEndIndex - 1
				beginCommit, endCommit := rf.checkCommit(rf.matchIndex, nextIndex, afterEndIndex)
				if beginCommit < endCommit {
					rf.logs.commit(endCommit-1, endCommit-1)
					DPrintf("node[%v]  Term[%v] from node[%v] args[%+v] commit[%v] in sync ", rf.me, rf.currentTerm, pNodeKey, *appendEntiresArgs, beginCommit)
				}
				rf.mu.Unlock()
			}
		}
	}
}
func (rf *Raft) checkCommit(pMatchIndex []int, pBeginSync int, pEndSync int) (rBeginCommit, rEndCommit int) {
	if len(pMatchIndex) == 1 {
		rBeginCommit = pBeginSync
		rEndCommit = pEndSync
		return
	}
	majority := len(pMatchIndex)/2 + 1
	sort.Slice(pMatchIndex, func(i, j int) bool { return pMatchIndex[i] > pMatchIndex[j] })
	rBeginCommit = pMatchIndex[majority] + 1
	rEndCommit = pMatchIndex[majority-1] + 1
	if pBeginSync > rBeginCommit {
		rBeginCommit = pBeginSync
	}
	if pEndSync < rEndCommit {
		rEndCommit = pEndSync
	}
	DPrintf("node[%v] commit[%v] checkCommit([%v], [%v], [%v])=[%v] [%v] in sync ", rf.me, rf.logs.getCommitIndex(), pMatchIndex, pBeginSync, pEndSync, rBeginCommit, rEndCommit)
	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.cancelFunc()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}

//单个线程收集完所有的结果以后，再返回，需要RPC支持超时机制
func (rf *Raft) sendRequestVoteToOtherPeers(pCurrentTerm, pLastLogIndex, pLastLogTerm int) {
	var mutex sync.Mutex
	voteSuccessNum := 1
	maxRespTerm := pCurrentTerm
	requestVoteArgs := &RequestVoteArgs{Term: pCurrentTerm, CandidateId: rf.me, LastLogIndex: pLastLogIndex, LastLogTerm: pLastLogTerm}
	for key, _ := range rf.peers {
		//如果没有tmpNode，会导致多个
		if key == rf.me {
			continue
		}
		onePeer := key
		go func() {
			var requestVoteReply RequestVoteReply
			if ok := rf.peers[onePeer].Call("Raft.RequestVote", requestVoteArgs, &requestVoteReply); ok {
				mutex.Lock()
				defer mutex.Unlock()
				DPrintf("node[%v] handle result from node[%v][%v]", rf.me, onePeer, requestVoteReply)
				if requestVoteReply.VoteGranted {
					voteSuccessNum++
					if maxRespTerm == pCurrentTerm && voteSuccessNum == rf.clusterMajority {
						rf.mu.Lock()
						DPrintf("node[%v] term[%v] become leader", rf.me, rf.currentTerm)
						rf.state = leader
						backIndex, _ := rf.logs.back()
						endIndex := backIndex + 1
						for key, _ := range rf.nextIndex {
							rf.nextIndex[key] = endIndex
						}
						for key, _ := range rf.matchIndex {
							rf.matchIndex[key] = 0
						}
						rf.mu.Unlock()

					}
				} else {
					if requestVoteReply.Term > maxRespTerm {
						maxRespTerm = requestVoteArgs.Term
						rf.mu.Lock()
						if maxRespTerm > rf.currentTerm {
							rf.currentTerm = maxRespTerm
							rf.state = follower
							rf.voteFor = -1
						}
						DPrintf("node[%v] term[%v] update to term[%v]", rf.me, rf.currentTerm, maxRespTerm)
						rf.mu.Unlock()
					}
				}
			}

		}()
	}
}
func (rf *Raft) ticker() {
	/*for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}*/
	firstRetryTime := rf.setNextRetryVote()
	sleepTimer := time.NewTimer(time.Duration(firstRetryTime) * time.Millisecond)
	for {
		select {
		case <-rf.cancelContext.Done():
			sleepTimer.Stop()
			return
		case now := <-sleepTimer.C:
			rf.mu.Lock()
			if rf.state == leader {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			rf.tryRequestVoteTimeMutex.RLock()
			nexTryTime := rf.tryRequestVoteTime
			rf.tryRequestVoteTimeMutex.RUnlock()
			if nexTryTime.After(now) {
				sleepTimer.Reset(nexTryTime.Sub(now))
				continue
			}
			rf.mu.Lock()
			rf.state = candidate
			rf.currentTerm++
			rf.voteFor = rf.me
			tmpCurrentTerm := rf.currentTerm
			lastLogIndex, lastLogTerm := rf.logs.back()
			rf.mu.Unlock()
			DPrintf("node[%v] Term[%v] Commit[%v] begin request vote", rf.me, tmpCurrentTerm, rf.logs.getCommitIndex())
			rf.sendRequestVoteToOtherPeers(tmpCurrentTerm, lastLogIndex, lastLogTerm)
		}
		nextSleep := rf.setNextRetryVote()
		sleepTimer.Reset(time.Duration(nextSleep) * time.Millisecond)
	}
}
func (rf *Raft) tickHeartBeat() {
	heartBeatTicker := time.NewTicker(20 * time.Millisecond)
	for {
		select {
		case <-rf.cancelContext.Done():
			heartBeatTicker.Stop()
			return
		case <-heartBeatTicker.C:
			rf.mu.Lock()
			tmpCurrentTerm := rf.currentTerm
			if rf.state != leader {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
			//DPrintf("node[%v] term[%v] send heartbeat", rf.me, tmpCurrentTerm)
			rf.sendHeartBeat(tmpCurrentTerm)
		}
	}
}

func (rf *Raft) sendHeartBeat(pCurrentTerm int) {
	var mutex sync.Mutex
	maxRespTerm := pCurrentTerm
	for key, _ := range rf.peers {
		if key == rf.me {
			continue
		}
		onePeerKey := key
		go func() {
			rf.mu.Lock()
			indexTerm, err := rf.logs.getTerm(rf.nextIndex[onePeerKey] - 1)
			if err != nil {
				DPrintf("Fatel rf.logs.getTerm(%v) Err[%v]", rf.nextIndex[onePeerKey], err)
			}
			AppendEntiresArgs := &AppendEntiresArgs{LeaderTerm: pCurrentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[onePeerKey] - 1, PrevLogTerm: indexTerm, LeaderCommit: rf.logs.getCommitIndex()}
			rf.mu.Unlock()
			var reply AppendEntiresReply
			if ok := rf.peers[onePeerKey].Call("Raft.AppendEntires", AppendEntiresArgs, &reply); ok {
				mutex.Lock()
				defer mutex.Unlock()
				if reply.Term > maxRespTerm {
					maxRespTerm = reply.Term
					rf.mu.Lock()
					rf.currentTerm = maxRespTerm
					rf.state = follower
					rf.voteFor = -1
					rf.mu.Unlock()
				} else if !reply.Success {
					//这里AppendEntiresArgs.PrevLogIndex不可能等于0
					rf.peersSyncRetryChannel[onePeerKey] <- syncRetryNotify{index: AppendEntiresArgs.PrevLogIndex, termNotMatch: true}
				}
			}
		}()
	}
}

func (rf *Raft) setNextRetryVote() int16 {
	ms := int16(150 + (rand.Int() % 151))
	now := time.Now()
	tryRequestVoteTime := now.Add(time.Duration(ms) * time.Millisecond)
	rf.tryRequestVoteTimeMutex.Lock()
	defer rf.tryRequestVoteTimeMutex.Unlock()
	if rf.tryRequestVoteTime.Before(tryRequestVoteTime) {
		rf.tryRequestVoteTime = tryRequestVoteTime
		return ms
	}
	return int16(rf.tryRequestVoteTime.Sub(tryRequestVoteTime))
}

func (rf *Raft) Run() {
	rf.nextIndex = make([]int, len(rf.peers))
	backIndex, _ := rf.logs.back()
	endIndex := backIndex + 1
	for key, _ := range rf.nextIndex {
		rf.nextIndex[key] = endIndex
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.cancelContext, rf.cancelFunc = context.WithCancel(context.Background())
	rf.peersSyncRetryChannel = make([]chan syncRetryNotify, len(rf.peers))
	for key := range rf.peersSyncRetryChannel {
		rf.peersSyncRetryChannel[key] = make(chan syncRetryNotify, 10)
	}
	//开启所有的ticker
	go rf.ticker()
	go rf.tickHeartBeat()
	for key, _ := range rf.peers {
		if key == rf.me {
			continue
		}
		onePeerKey := key
		go rf.tickSyncLog(onePeerKey)

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.clusterMajority = len(peers)/2 + 1

	// Your initialization code here (2A, 2B, 2C).

	rf.logs.init(1000, applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.Run()
	return rf
}
