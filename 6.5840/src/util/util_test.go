package util

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestDebug(t *testing.T) {
	rand.Seed(time.Now().Unix())
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	voteNum := 0
	voteSuccessNum := 0

	for i := 0; i < 10; i++ {
		go func() {
			mu.Lock()
			if rand.Intn(2) == 1 {
				voteSuccessNum++
			}
			voteNum++
			cond.Signal()
			mu.Unlock()
		}()
	}
	cond.L.Lock()
	for {
		if voteNum == 10 || voteSuccessNum > 5 {
			mu.Unlock()
			break
		}
		cond.Wait()
	}
	fmt.Printf("voteNum[%v] voteSuccessNum[%v]\n", voteNum, voteSuccessNum)
}
