package util

import (
	"fmt"
	"path"
	"runtime"
	"time"
)

type DebugType int

const (
	DebugRaft DebugType = iota
	DebugRaftSelectLeader
	DebugRaftLogCopy
	DebugRaftSnapshot
	DebugClient
	DebugMoveShardState
	DebugServerRaft
	DebugConfig
	DebugKV
	DebugKVSnapshot
	DebugShardctl
)

var gDebugSwitch map[DebugType]bool = map[DebugType]bool{DebugRaft: false, DebugClient: false, DebugServerRaft: true, DebugConfig: true, DebugMoveShardState: true, DebugKV: true, DebugRaftLogCopy: false, DebugRaftSnapshot: false, DebugRaftSelectLeader: false, DebugKVSnapshot: false, DebugShardctl: false}
var gDebugOneRaftNode = true
var gTotalSwitch = false
var gDebugBeginTime time.Time

func init() {
	gDebugBeginTime = time.Now()
}

func Debug(debugType DebugType, raftNode int, format string, a ...interface{}) {
	if !gTotalSwitch {
		return
	}
	if gDebugOneRaftNode && raftNode != 0 {
		return
	}
	if gDebugSwitch[debugType] {
		now := time.Now()
		_, file, line, _ := runtime.Caller(1)
		file = path.Base(file)
		deferTime := now.Sub(gDebugBeginTime)
		format = fmt.Sprintf("time:[%v] file:[%v] line:[%v] | %v\n", deferTime.Milliseconds(), file, line, format)
		fmt.Printf(format, a...)
	}
}
