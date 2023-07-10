package raft

import (
	"fmt"
	"testing"
)

func TestRaftLogInit(t *testing.T) {
	var rl raftLogs
	err := rl.init(1, nil)
	if err == nil {
		t.Fatalf("raftLogs.init(1) want not nil, equal nil")
	}
	err = rl.init(100, nil)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}

}

func TestRaftAppend(t *testing.T) {
	var rl raftLogs
	err := rl.init(3, nil)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}

	appendTerm := []int{1, 1, 2, 2, 2, 3, 3, 3}
	rPrevTerm := []int{0, 1, 1, 2, 2, 2, 3, 3}
	for key, value := range appendTerm {
		msgIndex, prevIndex, prevTerm, err := rl.append(OneRaftLog{Term: value})
		if err != nil || msgIndex != key+1 || prevIndex != key || prevTerm != rPrevTerm[key] {
			t.Fatalf("raftLogs.append({term:1}}) want 1, 0, 0,  nil, equal [%v], [%v], [%v] [%v]", msgIndex, prevIndex, prevIndex, err)
		}
		backIndex, backTerm := rl.back()
		if backIndex != msgIndex || backTerm != value {
			t.Fatalf("raftLogs.back() want [%v], equal [%v]", msgIndex, backIndex)
		}
		//比较 term < prevTerm, index < prevIndex
		if ok := rl.upToDateLast(value-1, msgIndex-1); ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value, value-1, msgIndex-1)
		}

		//比较 term < prevTerm, index == prevIndex
		if ok := rl.upToDateLast(value-1, msgIndex); ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value, value-1, msgIndex)
		}

		//比较 term < prevTerm, index > prevIndex
		if ok := rl.upToDateLast(value-1, msgIndex+1); ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value, value-1, msgIndex+1)
		}

		//比较 term == prevTerm, index < prevIndex
		if ok := rl.upToDateLast(value, msgIndex-1); ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value, value, msgIndex-1)
		}

		//比较 term == prevTerm, index == prevIndex
		if ok := rl.upToDateLast(value, msgIndex); !ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value, value, msgIndex)
		}

		//比较 term == prevTerm, index > prevIndex
		if ok := rl.upToDateLast(value, msgIndex+1); !ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value, value, msgIndex+1)
		}

		//比较 term > prevTerm, index < prevIndex
		if ok := rl.upToDateLast(value+1, msgIndex-1); !ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value+1, value, msgIndex-1)
		}

		//比较 term > prevTerm, index == prevIndex
		if ok := rl.upToDateLast(value+1, msgIndex); !ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value+1, value, msgIndex)
		}

		//比较 term > prevTerm, index > prevIndex
		if ok := rl.upToDateLast(value+1, msgIndex+1); !ok {
			t.Fatalf("back{index:[%v], term:[%v]}} rl.upToDateLast([%v], %v) want false, equal true", msgIndex, value+1, value, msgIndex+1)
		}

	}
}

func TestRaftGet(t *testing.T) {

	var rl raftLogs
	err := rl.init(100, nil)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}
	bufferTerm := []int{0, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5, 5, 5, 5, 6, 6}

	for i := 1; i < len(bufferTerm); i++ {
		rl.append(OneRaftLog{Term: bufferTerm[i]})
	}
	backIndex, _ := rl.back()
	for beginGetIndex := 1; beginGetIndex <= backIndex; beginGetIndex++ {
		for getNum := 1; getNum <= backIndex-beginGetIndex+3; getNum++ {
			compareNum := getNum
			if getNum > backIndex-beginGetIndex+1 {
				compareNum = backIndex - beginGetIndex + 1
			}
			prevTerm, logs, afterEndIndex, err := rl.get(beginGetIndex, getNum)
			if err != nil || prevTerm != bufferTerm[beginGetIndex-1] || afterEndIndex != beginGetIndex+compareNum {
				t.Fatalf("raftLogs.get([%v], [%v]) want {[%v], any, [%v],  [nil]} ; equal {[%v], any, [%v],  [%v]}", beginGetIndex, getNum, bufferTerm[beginGetIndex-1], beginGetIndex+getNum, prevTerm, afterEndIndex, err)
			}

			if len(logs) != compareNum {
				t.Fatalf("len([%v]) want [%v] ;  equal [%v]", logs, getNum, len(logs))
			}
			//比较log
			for i := 0; i < compareNum; i++ {
				if logs[i].Term != bufferTerm[beginGetIndex+i] {
					t.Fatalf("logs([%v]) want eual [%v] ;  not eual", logs[i], bufferTerm[beginGetIndex+i])
				}
			}
		}
	}
	//begin < 0
	_, _, _, err = rl.get(-1, 100)
	if err != ErrParamError {
		t.Fatalf("aftLogs.get([%v], [%v]) want nil ;  equal [%v]", 0, 100, err)
	}

	//begin == 0
	_, _, _, err = rl.get(0, 100)
	if err != ErrParamError {
		t.Fatalf("aftLogs.get([%v], [%v]) want nil ;  equal [%v]", 0, 100, err)
	}

}

func TestRaftInsert(t *testing.T) {
	var rl raftLogs
	err := rl.init(100, nil)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}
	bufferTerm := []int{1, 1, 2, 2, 2, 3, 3, 3}
	for _, value := range bufferTerm {
		rl.append(OneRaftLog{Term: value})
	}
	insertLogs := make([]OneRaftLog, 0)
	backIndex, backTerm := rl.back()
	// 测试心跳
	// prevIndex  > max
	err = rl.check(backTerm, backIndex+1)
	if err != ErrIndexGreaterThanMax {
		t.Fatalf("back{index:[%v], term:[%v]}} rl.insert([%v], [%v], any) want ErrIndexGreaterThanMax, equal [%v]", backIndex, backTerm, backTerm, backIndex+1, err)
	}

	// prevIndex = max, term < backTerm
	err = rl.check(backTerm-1, backIndex)
	if err != ErrPrevIndexAndPrevTermNotMatch {
		t.Fatalf("back{index:[%v], term:[%v]}} rl.insert([%v], [%v], any) want ErrPrevIndexAndPrevTermNotMatch, equal [%v]", backIndex, backTerm, backTerm-1, backIndex, err)
	}

	// prevIndex = max, term > backTerm
	err = rl.check(backTerm+1, backIndex)
	if err != ErrPrevIndexAndPrevTermNotMatch {
		t.Fatalf("back{index:[%v], term:[%v]}} rl.insert([%v], [%v], any) want ErrPrevIndexAndPrevTermNotMatch, equal [%v]", backIndex, backTerm, backTerm+1, backIndex, err)
	}
	// prevIndex = max, term == backTerm
	err = rl.check(backTerm, backIndex)
	if err != nil {
		t.Fatalf("back{index:[%v], term:[%v]}} rl.insert([%v], [%v], any) want nil, equal [%v]", backIndex, backTerm, backTerm, backIndex, err)
	}

	insertTerm := []int{4, 4, 5, 5, 5, 6, 6, 6}
	for _, value := range insertTerm {
		insertLogs = append(insertLogs, OneRaftLog{Term: value})
		bufferTerm = append(bufferTerm, value)
	}
	rl.insert(backTerm, backIndex, insertLogs)
	backIndex, backTerm = rl.back()
	if backIndex != len(bufferTerm) || backTerm != bufferTerm[len(bufferTerm)-1] {
		t.Fatalf("backIndex:[%v], backTerm:[%v] want == [%v] [%v]", backIndex, backTerm, len(insertTerm), bufferTerm[len(bufferTerm)-1])
	}

	_, getLogs, _, err := rl.get(1, len(bufferTerm))
	if err != nil {
		t.Fatalf("raftLogs.get(1, [%v]) want {any, any, any, [nil]} ; equal {any, any, any, [%v]}", len(bufferTerm), err)
	}
	for key, value := range getLogs {
		if value.Term != bufferTerm[key] {
			t.Fatalf("[%v] want equal [%v]", getLogs, bufferTerm)
		}
	}
	copy(bufferTerm, insertTerm)
	rl.insert(0, 0, insertLogs)
	_, getLogs, _, err = rl.get(1, len(bufferTerm))
	if err != nil {
		t.Fatalf("raftLogs.get(1, [%v]) want {any, any, any, [nil]} ; equal {any, any, any, [%v]}", len(bufferTerm), err)
	}

	bufferTerm = bufferTerm[:len(insertLogs)]
	for key, value := range getLogs {
		if value.Term != bufferTerm[key] {
			t.Fatalf("[%v] want equal [%v]", getLogs, bufferTerm)
		}
	}
	fmt.Printf("%v", getLogs)

}

func TestInsertTrunc(t *testing.T) {
	var rl raftLogs
	err := rl.init(100, nil)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}
	bufferTerm := []int{1, 1, 2, 2, 2, 3, 3, 3}
	for _, value := range bufferTerm {
		rl.append(OneRaftLog{Term: value})
	}
	insertTerm := []int{4, 4}
	insertLogs := []OneRaftLog{}
	for _, value := range insertTerm {
		insertLogs = append(insertLogs, OneRaftLog{Term: value})
	}
	rl.insert(2, 3, insertLogs)
	_, getLogs, _, err := rl.get(1, len(bufferTerm))
	if err != nil {
		t.Fatalf("raftLogs.get(1, [%v]) want {any, any, any, [nil]} ; equal {any, any, any, [%v]}", len(bufferTerm), err)
	}
	resultTerm := []int{1, 1, 2, 4, 4}
	if len(resultTerm) != len(getLogs) {
		t.Fatalf("[%v] want equal [%v]", len(resultTerm), len(getLogs))
	}
	for key, value := range getLogs {
		if value.Term != resultTerm[key] {
			t.Fatalf("[%v] want equal [%v]", getLogs, bufferTerm)
		}
	}
	insertTerm = []int{2, 4}
	insertLogs = []OneRaftLog{}
	for _, value := range insertTerm {
		insertLogs = append(insertLogs, OneRaftLog{Term: value})
	}
	rl.insert(1, 2, insertLogs)
	_, getLogs, _, err = rl.get(1, len(bufferTerm))
	if err != nil {
		t.Fatalf("raftLogs.get(1, [%v]) want {any, any, any, [nil]} ; equal {any, any, any, [%v]}", len(bufferTerm), err)
	}
	if len(resultTerm) != len(getLogs) {
		t.Fatalf("[%v] want equal [%v]", resultTerm, getLogs)
	}
	for key, value := range getLogs {
		if value.Term != resultTerm[key] {
			t.Fatalf("[%v] want equal [%v]", getLogs, bufferTerm)
		}
	}

}

func TestRaftCommit(t *testing.T) {
	var rl raftLogs
	bufferTerm := []int{1, 1, 2, 2, 2, 3, 3, 3}
	applyChan := make(chan ApplyMsg, len(bufferTerm))
	err := rl.init(100, applyChan)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}

	for _, value := range bufferTerm {
		rl.append(OneRaftLog{Term: value})
	}
	if rl.getCommitIndex() != 0 {
		t.Fatalf("rl.getCommitIndex() want 0, equal [%v]", rl.getCommitIndex())
	}

	err = rl.commit(3, 3)
	if err != nil {
		t.Fatalf("raftLogs.commit(3, 3) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 3 {
		t.Fatalf("rl.getCommitIndex() want 3, equal [%v]", rl.getCommitIndex())
	}
	err = rl.commit(2, 2)
	if err != nil {
		t.Fatalf("raftLogs.commit(2, 2) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 3 {
		t.Fatalf("rl.getCommitIndex() want 3, equal [%v]", rl.getCommitIndex())
	}
	err = rl.commit(4, 4)
	if err != nil {
		t.Fatalf("raftLogs.commit(4, 4) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 4 {
		t.Fatalf("rl.getCommitIndex() want 4, equal [%v]", rl.getCommitIndex())
	}
	for i := 1; i <= 4; i++ {
		applyMsg := <-applyChan
		if applyMsg.CommandIndex != i {
			t.Fatalf("applyMsg.CommandIndex[%v] want equal [%v]", applyMsg.CommandIndex, i)
		}
	}
	err = rl.commit(4, 5)
	if err != nil {
		t.Fatalf("raftLogs.commit(4, 5) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 4 {
		t.Fatalf("rl.getCommitIndex() want 4, equal [%v]", rl.getCommitIndex())
	}

	err = rl.commit(6, 5)
	if err != nil {
		t.Fatalf("raftLogs.commit(6, 5) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 5 {
		t.Fatalf("rl.getCommitIndex() want 5, equal [%v]", rl.getCommitIndex())
	}

	err = rl.commit(len(bufferTerm)+1, len(bufferTerm))
	if err != nil {
		t.Fatalf("raftLogs.commit( > maxIndex) want not nil, equal nil")
	}
	if rl.getCommitIndex() != len(bufferTerm) {
		t.Fatalf("rl.getCommitIndex() want %v, equal [%v]", len(bufferTerm), rl.getCommitIndex())
	}
}

func TestInsertAndCommit(t *testing.T) {
	var rl raftLogs
	applyChan := make(chan ApplyMsg, 1)
	err := rl.init(100, applyChan)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}
	rl.insert(0, 0, []OneRaftLog{OneRaftLog{1, 1}})
	err = rl.commit(1, 1)
	if err != nil {
		t.Fatalf("rl.commit(1) want nil, equal [%v]", err)
	}
	if len(applyChan) != 1 {
		t.Fatalf("len(applyChan) want 1, equal [%v]", len(applyChan))
	}
	cmd := <-applyChan
	if cmd.CommandIndex != 1 || cmd.Command != 1 {
		t.Fatalf("cmd.CommandIndex = %v cmd.Command = %v, want equal [1] [1]", cmd.CommandIndex, cmd.Command)
	}
}
