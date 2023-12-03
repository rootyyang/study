package raft

import (
	"errors"
)

var (
	ErrIndexLessThanMin             = errors.New("raftLog: Index Less than minimum")
	ErrIndexEqualMin                = errors.New("raftLog: Index equal to minimum")
	ErrIndexGreaterThanMax          = errors.New("raftLog: Index greater than maximum")
	ErrPrevIndexAndPrevTermNotMatch = errors.New("raftLog: Previous index and previous term not match")
	ErrTimeOut                      = errors.New("Time Out")
	ErrThereIsNewLeader             = errors.New("There is a new master node")
	ErrSpaceNotEnough               = errors.New("There is not enough space")
	ErrParamError                   = errors.New("Param Error")
	ErrAlreadyCommit                = errors.New("Already Commit")
)

type OneRaftLog struct {
	Term    int
	Command interface{}
}

//buffers[0] 固定的snapshot最后一个日志的index和对应的term
type raftLogs struct {
	buffers        []OneRaftLog
	lastApplied    int
	commitIndex    int
	posZeroIndex   int
	initBufferSize int
	snapshot       []byte
	notifyApplier  chan int
}

func (lc *raftLogs) init(pBufferLen uint32, pNotifyApplier chan int) error {
	if pBufferLen < 2 {
		return ErrParamError
	}
	lc.initBufferSize = int(pBufferLen)
	lc.lastApplied = -1
	lc.buffers = make([]OneRaftLog, 0, pBufferLen)
	lc.buffers = append(lc.buffers, OneRaftLog{Term: 0})
	lc.notifyApplier = pNotifyApplier
	return nil
}
func (lc *raftLogs) append(pOneLog OneRaftLog) (rMsgIndex int, rPrevIndex int, rPrevTerm int, rErr error) {

	rPrevIndex = lc.posZeroIndex + len(lc.buffers) - 1
	rPrevTerm = int(lc.buffers[len(lc.buffers)-1].Term)
	lc.buffers = append(lc.buffers, pOneLog)
	rMsgIndex = rPrevIndex + 1
	return
}

func (lc *raftLogs) insert(pPrevTerm int, pPrevIndex int, pLogs []OneRaftLog) {
	truncate := false
	var source, dest int
	if pPrevIndex <= lc.posZeroIndex {
		if pPrevIndex+len(pLogs) < lc.posZeroIndex {
			return
		}
		dest = 1
		source = lc.posZeroIndex - pPrevIndex
	} else {
		dest = pPrevIndex - lc.posZeroIndex + 1
		source = 0
	}
	for source < len(pLogs) {
		if dest < len(lc.buffers) {
			//覆盖
			if lc.buffers[dest].Term != pLogs[source].Term {
				lc.buffers[dest] = pLogs[source]
				truncate = true
			}
		} else {
			lc.buffers = append(lc.buffers, pLogs[source])
		}
		source++
		dest++
	}
	if truncate {
		lc.buffers = lc.buffers[:dest]
	}
}

//如果prevIndex < lc.posZeroIndex，则默认一定匹配
//理论上，prevIndex < lc.commitIndex，则默认一定匹配
func (lc *raftLogs) check(pPrevTerm int, pPrevIndex int) error {
	if pPrevIndex <= lc.commitIndex {
		return nil
	}
	if pPrevIndex >= lc.posZeroIndex+len(lc.buffers) {
		return ErrIndexGreaterThanMax
	}
	if lc.buffers[pPrevIndex-lc.posZeroIndex].Term != pPrevTerm {
		return ErrPrevIndexAndPrevTermNotMatch
	}
	return nil
}

func (lc *raftLogs) upToDateLast(pLogTerm int, pLogIndex int) bool {
	endIndex := lc.posZeroIndex + len(lc.buffers) - 1
	if pLogTerm > lc.buffers[len(lc.buffers)-1].Term {
		return true
	} else if pLogTerm == lc.buffers[len(lc.buffers)-1].Term {
		return pLogIndex >= endIndex
	}
	return false
}

func (lc *raftLogs) get(pBeginIndex int, pNum int) (rPrevTerm int, rLogs []OneRaftLog, rAfterEndIndex int, rErr error) {
	if pBeginIndex > lc.posZeroIndex+len(lc.buffers) || pBeginIndex <= lc.posZeroIndex {
		rErr = ErrParamError
		return
	}
	rPrevTerm = lc.buffers[pBeginIndex-lc.posZeroIndex-1].Term
	numInBuffers := len(lc.buffers) + lc.posZeroIndex - pBeginIndex
	if numInBuffers < pNum {
		pNum = numInBuffers
	}
	rAfterEndIndex = pBeginIndex + pNum
	beginCopy := pBeginIndex - lc.posZeroIndex
	rLogs = make([]OneRaftLog, pNum)
	for i := 0; i < pNum; i++ {
		rLogs[i] = lc.buffers[beginCopy]
		beginCopy++
	}
	return
}

func (lc *raftLogs) getCommitIndex() int {
	return lc.commitIndex
}

// 该函数返回pPrevIndex的Term的第一个entiry，如果已经是第一个，则返回上一个term的第一个
func (lc *raftLogs) getNextTryPrevIndex(pPrevIndex int) (rNextTryPrevIndex int, rNextTryPrevTerm int) {
	//前一个index的Term
	if pPrevIndex >= lc.posZeroIndex+len(lc.buffers) || pPrevIndex < lc.posZeroIndex {
		//理论上不可能出现这种情况，如果出现这种情况，直接报错
		rNextTryPrevIndex, rNextTryPrevTerm = lc.back()
		return
	}

	if pPrevIndex < lc.posZeroIndex {
		rNextTryPrevIndex = pPrevIndex
		return
	}

	if pPrevIndex == lc.posZeroIndex {
		rNextTryPrevIndex = lc.posZeroIndex
		rNextTryPrevTerm = lc.buffers[0].Term
		return
	}
	rNextTryPrevIndex = pPrevIndex - 1
	prevIndexPos := pPrevIndex - lc.posZeroIndex - 1
	prevTerm := lc.buffers[prevIndexPos+1].Term
	for ; rNextTryPrevIndex > lc.posZeroIndex && lc.buffers[prevIndexPos].Term == prevTerm; rNextTryPrevIndex-- {
		prevIndexPos--
	}
	rNextTryPrevTerm = lc.buffers[prevIndexPos].Term
	return
}
func (lc *raftLogs) matchTerm(pIndex, pTerm int) bool {

	if pIndex >= lc.posZeroIndex+len(lc.buffers) {
		return false
	}
	if pIndex < lc.posZeroIndex {
		return true
	}

	return lc.buffers[pIndex-lc.posZeroIndex].Term == pTerm
}
func (lc *raftLogs) commit(pCommitIndex, pNewestLogIndex int) error {
	if pCommitIndex <= lc.commitIndex || pNewestLogIndex <= lc.commitIndex {
		return nil
	}
	endIndex := pCommitIndex + 1
	if pCommitIndex >= pNewestLogIndex {
		endIndex = pNewestLogIndex + 1
	}
	lc.commitIndex = endIndex - 1
	commit := lc.commitIndex
	go func() { lc.notifyApplier <- commit }()
	return nil
}

func (lc *raftLogs) back() (rBackIndex int, rBackTerm int) {
	rBackIndex = lc.posZeroIndex + len(lc.buffers) - 1
	rBackTerm = lc.buffers[len(lc.buffers)-1].Term
	return
}

func (lc *raftLogs) getTerm(pIndex int) (rTerm int, rErr error) {
	if pIndex < lc.posZeroIndex || pIndex >= lc.posZeroIndex+len(lc.buffers) {
		rErr = ErrParamError
		return
	}
	pos := pIndex - lc.posZeroIndex
	rTerm = lc.buffers[pos].Term
	return
}

func (lc *raftLogs) getBuffers() []OneRaftLog {
	return lc.buffers
}
func (lc *raftLogs) setBuffers(pBuffers []OneRaftLog, pBeginIndex, pBeginTerm int) {
	lc.buffers = pBuffers
	lc.posZeroIndex = pBeginIndex
	lc.commitIndex = lc.posZeroIndex
	lc.buffers[0].Term = pBeginTerm
}
func (lc *raftLogs) setSnapshot(pSnapshot []byte) {
	lc.snapshot = pSnapshot
}
func (lc *raftLogs) getSnapshot() []byte {
	if lc.snapshot == nil || len(lc.snapshot) == 0 {
		return nil
	}
	return lc.snapshot
}
func (lc *raftLogs) begin() (int, int) {
	return lc.posZeroIndex, lc.buffers[0].Term
}

//保留最后一个
//没有更新commit index
func (lc *raftLogs) truncate(pBeginIndex int) {
	if pBeginIndex <= lc.posZeroIndex || pBeginIndex > lc.lastApplied {
		return
	}
	beginIndexPos := pBeginIndex - lc.posZeroIndex
	lc.buffers = lc.buffers[beginIndexPos:]
	lc.posZeroIndex = pBeginIndex
}
func (lc *raftLogs) intallSnapshot(pSnapshot []byte, pLastLogIndex, pLastLogTerm int) {
	if pLastLogIndex <= lc.posZeroIndex {
		return
	}
	lc.snapshot = pSnapshot
	lc.lastApplied = pLastLogIndex - 1
	if pLastLogIndex > lc.posZeroIndex+len(lc.buffers)-1 {
		lc.buffers = make([]OneRaftLog, 1, lc.initBufferSize)
		lc.buffers[0].Term = pLastLogTerm
		lc.commitIndex = pLastLogIndex
		lc.posZeroIndex = pLastLogIndex
	} else {
		if pLastLogIndex > lc.commitIndex {
			lc.commitIndex = pLastLogIndex
		}
		beginIndexPos := pLastLogIndex - lc.posZeroIndex
		lc.buffers = lc.buffers[beginIndexPos:]
		lc.posZeroIndex = pLastLogIndex
	}
	commit := lc.commitIndex
	go func() { lc.notifyApplier <- commit }()

}
func (lc *raftLogs) getAndUpdateApplier(pMaxCommitIndex int) []ApplyMsg {
	if pMaxCommitIndex > lc.commitIndex {
		pMaxCommitIndex = lc.commitIndex
	}
	if lc.lastApplied >= pMaxCommitIndex {
		return nil
	}
	rApplyMsg := make([]ApplyMsg, 0)
	if lc.lastApplied < lc.posZeroIndex {
		if lc.snapshot != nil && len(lc.snapshot) != 0 {
			rApplyMsg = append(rApplyMsg, ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: lc.snapshot, SnapshotIndex: lc.posZeroIndex, SnapshotTerm: lc.buffers[0].Term})
		}
		lc.lastApplied = lc.posZeroIndex
	}
	for beginPos := lc.lastApplied - lc.posZeroIndex + 1; lc.lastApplied < pMaxCommitIndex; beginPos++ {
		lc.lastApplied++
		rApplyMsg = append(rApplyMsg, ApplyMsg{CommandValid: true, Command: lc.buffers[beginPos].Command, CommandIndex: lc.lastApplied, SnapshotValid: false})

	}
	return rApplyMsg
}
