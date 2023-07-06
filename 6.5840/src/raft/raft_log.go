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
type RaftLogWithIndex struct {
	OneRaftLog
	Index int
}

type raftLogs struct {
	buffers []OneRaftLog
	//lastApplied        uint64 先跟commitIndex合并为一，如果是异步apply，则需要lastApplied
	commitIndex int
	applyCh     chan ApplyMsg
}

func (lc *raftLogs) init(pBufferLen uint32, pApplyCh chan ApplyMsg) error {
	if pBufferLen < 2 {
		return ErrParamError
	}
	lc.buffers = make([]OneRaftLog, 0, pBufferLen)
	lc.buffers = append(lc.buffers, OneRaftLog{Term: 0})
	lc.commitIndex = 0
	lc.applyCh = pApplyCh
	return nil
}
func (lc *raftLogs) append(pOneLog OneRaftLog) (rMsgIndex int, rPrevIndex int, rPrevTerm int, rErr error) {
	rPrevIndex = len(lc.buffers) - 1
	rPrevTerm = int(lc.buffers[rPrevIndex].Term)
	lc.buffers = append(lc.buffers, pOneLog)
	rMsgIndex = rPrevIndex + 1
	return
}

func (lc *raftLogs) insert(pPrevTerm int, pPrevIndex int, logs []OneRaftLog) {
	beginCopyPos := pPrevIndex + 1
	for _, value := range logs {
		if beginCopyPos < len(lc.buffers) {
			//覆盖
			lc.buffers[beginCopyPos] = value
		} else {
			lc.buffers = append(lc.buffers, value)
		}
		beginCopyPos++
	}
	return
}

func (lc *raftLogs) check(pPrevTerm int, pPrevIndex int) error {
	if pPrevIndex >= len(lc.buffers) {
		return ErrIndexGreaterThanMax
	}

	if lc.buffers[pPrevIndex].Term != pPrevTerm {
		return ErrPrevIndexAndPrevTermNotMatch
	}
	return nil
}

func (lc *raftLogs) upToDateLast(pLogTerm int, pLogIndex int) bool {
	endIndex := len(lc.buffers) - 1
	if pLogTerm > lc.buffers[endIndex].Term {
		return true
	} else if pLogTerm == lc.buffers[endIndex].Term {
		if pLogIndex >= endIndex {
			return true
		}
		return false
	}
	return false
}

func (lc *raftLogs) get(pBeginIndex int, pNum int) (rPrevTerm int, rLogs []OneRaftLog, rAfterEndIndex int, rErr error) {
	if pBeginIndex >= len(lc.buffers) || pBeginIndex <= 0 {
		rErr = ErrParamError
		return
	}
	rPrevTerm = lc.buffers[pBeginIndex-1].Term
	numInBuffers := len(lc.buffers) - pBeginIndex
	if numInBuffers < pNum {
		pNum = numInBuffers
	}
	rAfterEndIndex = pBeginIndex + pNum
	rLogs = make([]OneRaftLog, pNum)
	for i := 0; i < pNum; i++ {
		rLogs[i] = lc.buffers[pBeginIndex]
		pBeginIndex++
	}
	return
}

func (lc *raftLogs) getCommitIndex() int {
	return lc.commitIndex
}

// 如果该index是本term的第一个，则下一次尝试上一个term的第一个，否则尝试本term的第一个
func (lc *raftLogs) getNextTryWhenAppendEntiresFalse(pIndex int) (rNextTryIndex int, rErr error) {
	//前一个index的Term
	if pIndex >= len(lc.buffers) || pIndex <= 0 {
		rErr = ErrParamError
		return
	}
	beforeIndex := pIndex - 1
	findFirstTerm := lc.buffers[beforeIndex].Term
	rNextTryIndex = pIndex
	for ; beforeIndex > 0 && lc.buffers[beforeIndex].Term == findFirstTerm; rNextTryIndex-- {
		beforeIndex--
	}
	return
}
func (lc *raftLogs) commit(pCommitIndex int) error {
	if pCommitIndex <= lc.commitIndex {
		return nil
	}
	endIndex := pCommitIndex + 1
	if pCommitIndex >= len(lc.buffers) {
		endIndex = len(lc.buffers)
	}
	for key, value := range lc.buffers[lc.commitIndex+1 : endIndex] {
		lc.applyCh <- ApplyMsg{CommandValid: true, Command: value.Command, CommandIndex: lc.commitIndex + 1 + key}
	}
	lc.commitIndex = endIndex - 1
	return nil
}

func (lc *raftLogs) back() (rBackIndex int, rBackTerm int) {
	rBackIndex = len(lc.buffers) - 1
	rBackTerm = lc.buffers[rBackIndex].Term
	return
}
