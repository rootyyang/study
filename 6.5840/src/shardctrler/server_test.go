package shardctrler

import "testing"

func TestJoinGroup(t *testing.T) {
	//只有一个group，增加3个gourp
	config := Config{Num: 0}
	config.Groups = make(map[int][]string)
	config.Groups[0] = nil
	joinGroups := make(map[int][]string, 0)
	for i := 1; i <= 3; i++ {
		joinGroups[i] = nil
	}
	newConfig, err := JoinGroups(config, joinGroups)
	if err != nil || newConfig.Shards == config.Shards {
		t.Fatalf("want oldConfig.Shards[%v] != newConfig.Shards[%v]", config.Shards, newConfig.Shards)
	}
	if len(newConfig.Groups) == len(config.Groups) || len(newConfig.Groups) != 4 {
		t.Fatalf("want oldConfig.Groups[%v] != newConfig.Groups[%v]", config.Groups, newConfig.Groups)
	}
	group2ShardsNum := make(map[int]int, 4)
	for _, value := range newConfig.Shards {
		group2ShardsNum[value]++
	}
	has2ShardGroupNum, has3ShardGroupNum := 0, 0
	for _, value := range group2ShardsNum {
		if value == 2 {
			has2ShardGroupNum++
		} else if value == 3 {
			has3ShardGroupNum++
		} else {
			t.Fatalf("shard num error [%v]", value)
		}
	}
	if has2ShardGroupNum != 2 || has3ShardGroupNum != 2 {
		t.Fatalf("newConfig.Shards[%v] group2ShardsNum[%v] has2ShardGroupNum[%v] has3ShardGroupNum[%v]", newConfig.Shards, group2ShardsNum, has2ShardGroupNum, has3ShardGroupNum)
	}
	tryAgainConfig, err := JoinGroups(config, joinGroups)
	if err != nil || tryAgainConfig.Shards != newConfig.Shards {
		t.Fatalf("tryAgainConfig.Shards[%v] != newConfig.Shards[%v]", tryAgainConfig.Shards, newConfig.Shards)
	}

	//测试已经在
	_, err = JoinGroups(newConfig, joinGroups)
	if err == nil {
		t.Fatalf("JoinGroups(newConfig, joinGroups) error != nil")
	}

}
func TestLeaveGroup(t *testing.T) {
	config := Config{Num: 0}
	config.Groups = make(map[int][]string)
	config.Groups[0] = nil
	for i := 0; i <= 6; i++ {
		config.Groups[i] = nil
	}
	leaveGroups := []int{5, 6}
	newConfig, err := LeaveGroups(config, leaveGroups)
	if err != nil || newConfig.Shards == config.Shards {
		t.Fatalf("want oldConfig.Shards[%v] != newConfig.Shards[%v]", config.Shards, newConfig.Shards)
	}
	if len(newConfig.Groups) == len(config.Groups) || len(newConfig.Groups) != 5 {
		t.Fatalf("want oldConfig.Groups[%v] != newConfig.Groups[%v]", config.Groups, newConfig.Groups)
	}
	group2ShardsNum := make(map[int]int, 4)
	for _, value := range newConfig.Shards {
		group2ShardsNum[value]++
	}
	has2ShardGroupNum := 0
	for _, value := range group2ShardsNum {
		if value == 2 {
			has2ShardGroupNum++
		} else {
			t.Fatalf("shard num error [%v]", value)
		}
	}
	if has2ShardGroupNum != 5 {
		t.Fatalf("newConfig.Shards[%v] group2ShardsNum[%v] has2ShardGroupNum[%v]", newConfig.Shards, group2ShardsNum, has2ShardGroupNum)
	}
	tryAgainConfig, err := LeaveGroups(config, leaveGroups)
	if err != nil || tryAgainConfig.Shards != newConfig.Shards {
		t.Fatalf("tryAgainConfig.Shards[%v] != newConfig.Shards[%v]", tryAgainConfig.Shards, newConfig.Shards)
	}
}
func TestLeaveRebalance(t *testing.T) {
	//测试正常的均衡
	config := Config{Num: 0}
	config.Groups = make(map[int][]string)
	config.Groups[0] = nil
	for i := 0; i <= 3; i++ {
		config.Groups[i] = nil
	}
	rebalance(&config)
	group2ShardsNum := make(map[int]int, 4)
	for _, value := range config.Shards {
		group2ShardsNum[value]++
	}
	has2ShardGroupNum, has3ShardGroupNum := 0, 0
	for _, value := range group2ShardsNum {
		if value == 2 {
			has2ShardGroupNum++
		} else if value == 3 {
			has3ShardGroupNum++
		} else {
			t.Fatalf("shard num error [%v] config.Shards[%v]", value, config.Shards)
		}
	}
	if has2ShardGroupNum != 2 || has3ShardGroupNum != 2 {
		t.Fatalf("config.Shards[%v] ", config.Shards)
	}
	newShards := [NShards]int{1, 2, 3, 4, 4, 4, 4, 4, 4, 4}
	config.Shards = newShards
	rebalance(&config)
	group2ShardsNum = make(map[int]int, 4)
	for _, value := range config.Shards {
		group2ShardsNum[value]++
	}
	has2ShardGroupNum, has3ShardGroupNum = 0, 0
	for _, value := range group2ShardsNum {
		if value == 2 {
			has2ShardGroupNum++
		} else if value == 3 {
			has3ShardGroupNum++
		} else {
			t.Fatalf("shard num error [%v] config.Shards[%v]", value, config.Shards)
		}
	}
	if has2ShardGroupNum != 2 || has3ShardGroupNum != 2 {
		t.Fatalf("config.Shards[%v] ", config.Shards)
	}

}
