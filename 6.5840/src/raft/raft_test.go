package raft

import (
	"reflect"
	"testing"
)

func TestRaftCheckCommit(t *testing.T) {
	//该函数用于定义数据
	getDataAndCheckCommit(t, []int{1, 2, 3}, 1, 3, 2, 3)
	getDataAndCheckCommit(t, []int{3, 3, 3}, 1, 3, 4, 3)
	getDataAndCheckCommit(t, []int{3, 3, 4}, 1, 3, 4, 3)
	getDataAndCheckCommit(t, []int{2, 3, 3}, 1, 3, 3, 3)
	getDataAndCheckCommit(t, []int{1, 2, 3, 4, 5}, 1, 4, 3, 4)

}
func getDataAndCheckCommit(t *testing.T, pMatchIndex []int, pBeginSync, pEndSync, pExpectBeginCommit, pExpectEndCommit int) {
	rf := &Raft{}
	beginCommit, endCommit := rf.checkCommit(pMatchIndex, pBeginSync, pEndSync)
	if beginCommit != pExpectBeginCommit || endCommit != pExpectEndCommit {
		t.Fatalf("rf.checkCommit(%v, %v, %v) = %v, %v ; want equal %v, %v", pMatchIndex, pBeginSync, pEndSync, beginCommit, endCommit, pExpectBeginCommit, pExpectEndCommit)
	}
}
func TestPersister(t *testing.T) {
	rf := &Raft{}
	rf.currentTerm = 10
	rf.voteFor = 2
	rf.persister = MakePersister()
	rf.logs.init(10, nil)
	bufferTerm := []int{1, 1, 2, 2, 2, 3, 3, 3}
	for _, value := range bufferTerm {
		rf.logs.append(OneRaftLog{Term: value, Command: value})
	}
	rf.persist()

	rfCopy := &Raft{}
	rfCopy.persister = rf.persister
	rfCopy.readPersist(rfCopy.persister.ReadRaftState())
	if rfCopy.currentTerm != rf.currentTerm {
		t.Fatalf("rfCopy.currentTerm[%v] != rf.currentTerm[%v]", rfCopy.currentTerm, rf.currentTerm)
	}
	if rfCopy.voteFor != rf.voteFor {
		t.Fatalf("rfCopy.voteFor[%v] != rf.voteFor[%v]", rfCopy.voteFor, rf.voteFor)
	}
	if len(rfCopy.logs.getBuffers()) != len(rf.logs.getBuffers()) {
		t.Fatalf("rfCopy.logs.buffers[%v] != rf.logs.buffers[%v]", rfCopy.logs.getBuffers(), rf.logs.getBuffers())
	}
	if !reflect.DeepEqual(rf.logs.getBuffers(), rfCopy.logs.getBuffers()) {
		t.Fatalf("rfCopy.logs.buffers[%v] != rf.logs.buffers[%v]", rfCopy.logs.getBuffers(), rf.logs.getBuffers())
	}
	//fmt.Printf("[%v]", rfCopy.logs.getBuffers())

}
