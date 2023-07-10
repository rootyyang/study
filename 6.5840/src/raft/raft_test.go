package raft

import "testing"

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
