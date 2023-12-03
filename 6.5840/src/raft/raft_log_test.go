package raft

import (
	"bytes"
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
	bufferTerm := []int{1, 1, 2, 2, 2, 3, 3, 3, 3}
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
		t.Fatalf("backIndex:[%v], backTerm:[%v] want == [%v] [%v]", backIndex, backTerm, len(bufferTerm), bufferTerm[len(bufferTerm)-1])
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
	//fmt.Printf("%v", getLogs)

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
	notify := make(chan int)
	err := rl.init(100, notify)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}

	for _, value := range bufferTerm {
		rl.append(OneRaftLog{Term: value, Command: value})
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
	commit := <-notify
	if commit != 3 {
		t.Fatalf("notify want 3, equal [%v]", commit)
	}

	err = rl.commit(2, 2)
	if err != nil {
		t.Fatalf("raftLogs.commit(2, 2) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 3 {
		t.Fatalf("rl.getCommitIndex() want 3, equal [%v]", rl.getCommitIndex())
	}
	if len(notify) != 0 {
		t.Fatalf("len(notify) want 0, equal [%v]", len(notify))
	}

	err = rl.commit(4, 4)
	if err != nil {
		t.Fatalf("raftLogs.commit(4, 4) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 4 {
		t.Fatalf("rl.getCommitIndex() want 4, equal [%v]", rl.getCommitIndex())
	}
	commit = <-notify
	if commit != 4 {
		t.Fatalf("notify want 4, equal [%v]", commit)
	}
	err = rl.commit(4, 5)
	if err != nil {
		t.Fatalf("raftLogs.commit(4, 5) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 4 {
		t.Fatalf("rl.getCommitIndex() want 4, equal [%v]", rl.getCommitIndex())
	}
	if len(notify) != 0 {
		t.Fatalf("len(notify) want 0, equal [%v]", len(notify))
	}
	err = rl.commit(6, 5)
	if err != nil {
		t.Fatalf("raftLogs.commit(6, 5) want nil, equal [%v]", err)
	}
	if rl.getCommitIndex() != 5 {
		t.Fatalf("rl.getCommitIndex() want 5, equal [%v]", rl.getCommitIndex())
	}
	commit = <-notify
	if commit != 5 {
		t.Fatalf("notify want 4, equal [%v]", commit)
	}

	rApplySlice := rl.getAndUpdateApplier(6)
	if len(rApplySlice) != 5 {
		t.Fatalf("rl.getAndUpdateApplier(6) equal [%v]", rApplySlice)
	}
	for key, value := range rApplySlice {
		if value.CommandIndex != key+1 || value.Command != bufferTerm[key] {
			t.Fatalf("rl.getAndUpdateApplier(6) equal [%v]", rApplySlice)
		}
	}

	err = rl.commit(len(bufferTerm)+1, len(bufferTerm))
	if err != nil {
		t.Fatalf("raftLogs.commit( > maxIndex) want not nil, equal nil")
	}
	if rl.getCommitIndex() != len(bufferTerm) {
		t.Fatalf("rl.getCommitIndex() want %v, equal [%v]", len(bufferTerm), rl.getCommitIndex())
	}
	commit = <-notify
	if commit != len(bufferTerm) {
		t.Fatalf("notify want [%v], equal [%v]", len(bufferTerm), commit)
	}

	rApplySlice = rl.getAndUpdateApplier(len(bufferTerm))
	if len(rApplySlice) != len(bufferTerm)-5 {
		t.Fatalf("rl.getAndUpdateApplier(6) equal [%v]", rApplySlice)
	}
	for key, value := range rApplySlice {
		if value.CommandIndex != key+6 || value.Command != bufferTerm[key+5] {
			t.Fatalf("rl.getAndUpdateApplier(6) equal [%v]", rApplySlice)
		}
	}
}

func TestGetNextTry(t *testing.T) {
	var rl raftLogs
	bufferTerm := []int{1, 1, 2, 2, 2, 3, 3, 3}
	err := rl.init(100, nil)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}
	for _, value := range bufferTerm {
		rl.append(OneRaftLog{Term: value})
	}
	backIndex, backTerm := rl.back()
	prevIndex, prevIndexTerm := rl.getNextTryPrevIndex(10)
	if prevIndex != backIndex || prevIndexTerm != backTerm {
		t.Fatalf("raftLogs.getNextTryIndex(10) want [%v], [%v]; equal [%v] [%v]", backIndex, backTerm, prevIndex, prevIndexTerm)
	}
	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(-1)
	if prevIndex != backIndex || prevIndexTerm != backTerm {
		t.Fatalf("raftLogs.getNextTryIndex(-1) want [%v], [%v]; equal [%v] [%v]", backIndex, backTerm, prevIndex, prevIndexTerm)
	}

	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(0)
	if prevIndex != 0 || prevIndexTerm != 0 {
		t.Fatalf("raftLogs.getNextTryIndex(0) want [%v], [%v]; equal 0 0", backIndex, backTerm)
	}

	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(1)
	if prevIndex != 0 || prevIndexTerm != 0 {
		t.Fatalf("raftLogs.getNextTryIndex(1) want 0, 0 equal [%v], [%v];", prevIndex, prevIndexTerm)
	}
	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(2)
	if prevIndex != 0 || prevIndexTerm != 0 {
		t.Fatalf("raftLogs.getNextTryIndex(2) want 0, 0 equal [%v], [%v];", prevIndex, prevIndexTerm)
	}
	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(3)
	if prevIndex != 2 || prevIndexTerm != 1 {
		t.Fatalf("raftLogs.getNextTryIndex(3) want 2, 1 equal [%v], [%v];", prevIndex, prevIndexTerm)
	}
	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(4)
	if prevIndex != 2 || prevIndexTerm != 1 {
		t.Fatalf("raftLogs.getNextTryIndex(0) want [%v], [%v]; equal 2 1", backIndex, backTerm)
	}
	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(5)
	if prevIndex != 2 || prevIndexTerm != 1 {
		t.Fatalf("raftLogs.getNextTryIndex(0) want [%v], [%v]; equal 2 1", backIndex, backTerm)
	}

	bufferTerm = []int{1, 2, 3, 4, 5, 6}
	err = rl.init(100, nil)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}
	for _, value := range bufferTerm {
		rl.append(OneRaftLog{Term: value})
	}
	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(6)
	if prevIndex != 5 || prevIndexTerm != 5 {
		t.Fatalf("raftLogs.getNextTryIndex(0) want [%v], [%v]; equal 5 5", backIndex, backTerm)
	}
	prevIndex, prevIndexTerm = rl.getNextTryPrevIndex(2)
	if prevIndex != 1 || prevIndexTerm != 1 {
		t.Fatalf("raftLogs.getNextTryIndex(0) want [%v], [%v]; equal 1 1", backIndex, backTerm)
	}
}

func TestTruncate(t *testing.T) {
	var rl raftLogs
	bufferTerm := []int{1, 1, 2, 2, 2, 3, 3, 3}
	notify := make(chan int)
	err := rl.init(100, notify)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}
	for _, value := range bufferTerm {
		rl.append(OneRaftLog{Term: value, Command: value})
	}
	//truncate
	rl.truncate(-1)
	beginIndex, beginTerm := rl.begin()
	if beginIndex != 0 || beginTerm != 0 {
		t.Fatalf("rl.begin() want 0, 0, equal [%v] [%v]", beginIndex, beginTerm)
	}

	backIndex, backTerm := rl.back()
	if backIndex != 8 || backTerm != 3 {
		t.Fatalf("rl.begin() want 8, 3, equal [%v] [%v]", backIndex, backTerm)
	}
	rl.commit(4, 4)
	if rl.getCommitIndex() != 4 {
		t.Fatalf("rl.getCommitIndex() want 4, equal [%v]", rl.getCommitIndex())
	}
	commit := <-notify
	if commit != 4 {
		t.Fatalf("notify want 4, equal [%v]", commit)
	}
	rl.truncate(5)
	beginIndex, beginTerm = rl.begin()
	if beginIndex != 0 || beginTerm != 0 {
		t.Fatalf("rl.begin() want 0, 0, equal [%v] [%v]", beginIndex, beginTerm)
	}

	backIndex, backTerm = rl.back()
	if backIndex != 8 || backTerm != 3 {
		t.Fatalf("rl.begin() want 8, 3, equal [%v] [%v]", backIndex, backTerm)
	}

	rApplySlice := rl.getAndUpdateApplier(4)
	if len(rApplySlice) != 4 {
		t.Fatalf("rl.getAndUpdateApplier(6) equal [%v]", rApplySlice)
	}
	for key, value := range rApplySlice {
		if value.CommandIndex != key+1 || value.Command != bufferTerm[key] {
			t.Fatalf("rl.getAndUpdateApplier(4) equal [%v]", rApplySlice)
		}
	}
	rl.truncate(3)
	beginIndex, beginTerm = rl.begin()
	if beginIndex != 3 || beginTerm != 2 {
		t.Fatalf("rl.begin() want 0, 0, equal [%v] [%v]", beginIndex, beginTerm)
	}

	backIndex, backTerm = rl.back()
	if backIndex != 8 || backTerm != 3 {
		t.Fatalf("rl.begin() want 8, 3, equal [%v] [%v]", backIndex, backTerm)
	}
	rl.append(OneRaftLog{Term: 4})
	backIndex, backTerm = rl.back()
	if backIndex != 9 || backTerm != 4 {
		t.Fatalf("rl.begin() want 8, 3, equal [%v] [%v]", backIndex, backTerm)
	}

	if err = rl.check(100, 2); err != nil {
		t.Fatalf("rl.check(2, 100) want nil, equal [%v]", err)
	}
	if err = rl.check(2, 3); err != nil {
		t.Fatalf("rl.check(2, 3) want nil, equal [%v]", err)
	}

	if err = rl.check(3, 3); err != nil {
		t.Fatalf("rl.check(3, 3) want nil, equal [%v]]", err)
	}
	if err = rl.check(2, 5); err != nil {
		t.Fatalf("rl.check(2, 5) want nil, equal [%v]]", err)
	}

	if err = rl.check(3, 5); err == nil {
		t.Fatalf("rl.check(2, 3) want not nil, equal [%v]]", err)
	}

	newBufferTerm := []int{4, 4, 5, 5, 5, 6, 6}
	insertLogs := []OneRaftLog{}
	for _, value := range newBufferTerm {
		insertLogs = append(insertLogs, OneRaftLog{Term: value, Command: value})
	}
	rl.insert(2, 3, insertLogs)
	_, _, _, err = rl.get(3, 200)
	if err == nil {
		t.Fatalf("rl.get(3, 200) want _, _, _, nil  equal _, _, _,[%v]", err)
	}
	prevTerm, logs, afterEndIndex, err := rl.get(4, 200)
	if err != nil || prevTerm != 2 || afterEndIndex != 11 {
		t.Fatalf("rl.get(3, 200) want 2, any, 11,  nil  equal %v, any, %v , %v", prevTerm, afterEndIndex, err)
	}
	for key, value := range logs {
		if newBufferTerm[key] != value.Term {
			t.Fatalf("rl.get(3, 200) want any, [%v], any, nil  equal any, [%v], any, nil", logs, newBufferTerm)
		}
	}
	if !rl.upToDateLast(6, 10) {
		t.Fatalf("rl.upToDateLast(6, 10) want true, equal false")
	}
	if !rl.upToDateLast(7, 10) {
		t.Fatalf("rl.upToDateLast(7, 10) want true, equal false")
	}
	if rl.upToDateLast(5, 10) {
		t.Fatalf("rl.upToDateLast(6, 10) want false, equal true")
	}

	nextTryPrevIndex, nextTryPrevTerm := rl.getNextTryPrevIndex(10)
	if nextTryPrevIndex != 8 || nextTryPrevTerm != 5 {
		t.Fatalf("rl.getNextTryPrevIndex(10) want 8, 5, equal [%v], [%v]", nextTryPrevIndex, nextTryPrevTerm)
	}

	nextTryPrevIndex, nextTryPrevTerm = rl.getNextTryPrevIndex(4)
	if nextTryPrevIndex != 3 || nextTryPrevTerm != 2 {
		t.Fatalf("rl.getNextTryPrevIndex(4) want 3, 2, equal [%v], [%v]", nextTryPrevIndex, nextTryPrevTerm)
	}
	nextTryPrevIndex, nextTryPrevTerm = rl.getNextTryPrevIndex(3)
	if nextTryPrevIndex != 3 || nextTryPrevTerm != 2 {
		t.Fatalf("rl.getNextTryPrevIndex(3) want 3, 2, equal [%v], [%v]", nextTryPrevIndex, nextTryPrevTerm)
	}
	if !rl.matchTerm(3, 2) {
		t.Fatalf("rl.matchTerm(3, 2) want true, equal false")
	}
	if !rl.matchTerm(4, 4) {
		t.Fatalf("rl.matchTerm(3, 2) want true, equal false")
	}
	tTerm, err := rl.getTerm(3)
	if err != nil || tTerm != 2 {
		t.Fatalf("rl.getTerm(3) want 2, nil ; equal [%v] [%v]", tTerm, err)
	}
	tTerm, err = rl.getTerm(4)
	if err != nil || tTerm != 4 {
		t.Fatalf("rl.getTerm(4) want 4, nil ; equal [%v] [%v]", tTerm, err)
	}
	rl.commit(5, 5)
	if rl.getCommitIndex() != 5 {
		t.Fatalf("rl.getCommitIndex() want 5, equal [%v]", rl.getCommitIndex())
	}
	commit = <-notify
	if commit != 5 {
		t.Fatalf("notify want 5, equal [%v]", commit)
	}
	rApplySlice = rl.getAndUpdateApplier(5)
	if len(rApplySlice) != 1 || rApplySlice[0].CommandIndex != 5 || rApplySlice[0].Command != 4 {
		t.Fatalf("rl.getAndUpdateApplier(5) equal [%+v]", rApplySlice)
	}

}

func TestInstallSnapshot(t *testing.T) {
	var rl raftLogs
	bufferTerm := []int{1, 1, 2, 2, 2, 3, 3, 3}
	notify := make(chan int)
	err := rl.init(100, notify)
	if err != nil {
		t.Fatalf("raftLogs.init(100) want nil, equal [%v]", err)
	}
	for _, value := range bufferTerm {
		rl.append(OneRaftLog{Term: value, Command: value})
	}
	snapshot := make([]byte, 20)
	rl.intallSnapshot(snapshot, len(bufferTerm)+1, 4)
	beginIndex, beginTerm := rl.begin()
	if beginIndex != len(bufferTerm)+1 || beginTerm != 4 {
		t.Fatalf("rl.begin() want [%v], 4 equal [%v], [%v]", len(bufferTerm)+1, beginIndex, beginTerm)
	}
	commit := rl.getCommitIndex()
	if commit != beginIndex {
		t.Fatalf("rl.getCommitIndex() want [%v], equal [%v]", beginIndex, commit)
	}
	notifyCommit := <-notify
	if notifyCommit != commit {
		t.Fatalf("notify want [%v], equal [%v]", commit, notifyCommit)
	}

	rApplySlice := rl.getAndUpdateApplier(commit)
	if len(rApplySlice) != 1 || rApplySlice[0].CommandValid != false || !bytes.Equal(rApplySlice[0].Snapshot, snapshot) || rApplySlice[0].SnapshotIndex != beginIndex || rApplySlice[0].SnapshotTerm != beginTerm || rApplySlice[0].SnapshotValid != true {
		t.Fatalf("rl.rl.getAndUpdateApplier  equal [%+v]", rApplySlice)
	}

	newBufferTerm := []int{4, 4, 5, 5, 5, 6, 6, 6}
	for _, value := range newBufferTerm {
		rl.append(OneRaftLog{Term: value, Command: value})
	}
	rl.commit(commit+4, commit+4)
	notifyCommit = <-notify
	if notifyCommit != commit+4 {
		t.Fatalf("notify want [%v], equal [%v]", commit, notifyCommit)
	}
	commit = commit + 4
	snapshot[0] = 1
	rl.intallSnapshot(snapshot, commit+1, newBufferTerm[4])
	beginIndex, beginTerm = rl.begin()
	if beginIndex != commit+1 || beginTerm != newBufferTerm[4] {
		t.Fatalf("rl.begin() want [%v], [%v] equal [%v], [%v]", commit+1, newBufferTerm[5], beginIndex, beginTerm)
	}
	newCommit := rl.getCommitIndex()
	if newCommit != commit+1 {
		t.Fatalf("rl.getCommitIndex() want [%v], equal [%v]", newCommit, commit+1)
	}
	notifyCommit = <-notify
	if notifyCommit != newCommit {
		t.Fatalf("notify want [%v], equal [%v]", newCommit, notifyCommit)
	}
	rApplySlice = rl.getAndUpdateApplier(newCommit)
	if len(rApplySlice) != 1 || rApplySlice[0].CommandValid != false || !bytes.Equal(rApplySlice[0].Snapshot, snapshot) || rApplySlice[0].SnapshotIndex != beginIndex || rApplySlice[0].SnapshotTerm != beginTerm || rApplySlice[0].SnapshotValid != true {
		t.Fatalf("rl.rl.getAndUpdateApplier  equal [%+v]", rApplySlice)
	}

}
