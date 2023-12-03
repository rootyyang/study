package mr

import (
	"fmt"
	"strconv"
	"testing"
)

func TestMapTaskHandler(t *testing.T) {
	MapTaskHandle(func(pFilename string, pContent string) []KeyValue {
		if pFilename != "./rpc.go" {
			return nil
		}
		if len(pContent) == 0 {
			return nil
		}

		return []KeyValue{{"key1", "1"}, {"key1", "1"}, {"key2", "1"}, {"key3", "1"}, {"key4", "1"}}
	}, 0, 5, "./rpc.go")

	ReduceTaskHandle(func(pKey string, pValues []string) string {
		sum := 0
		for _, value := range pValues {
			i, e := strconv.Atoi(value)
			if e != nil {
				continue
			}
			sum += i
		}
		return fmt.Sprintf("%v", sum)
	}, 3, 1)

}
