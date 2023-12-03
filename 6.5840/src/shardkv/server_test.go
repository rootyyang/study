package shardkv

import (
	"bytes"
	"testing"

	"6.5840/labgob"
)

func TestEncodeAndDecode(t *testing.T) {
	kvShards := make(map[int]KVShard)
	firstShard := KVShard{}
	firstShard.MemTable = make(map[string]string)
	firstShard.MemTable["test2"] = "value2"
	kvShards[0] = firstShard

	byteBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(byteBuffer)
	err := encoder.Encode(kvShards)
	if err != nil {
		t.Fatalf("encode memtable[%v] error", kvShards)
	}

	memtableBytes := byteBuffer.Bytes()

	readBuffer := bytes.NewBuffer(memtableBytes)
	decoder := labgob.NewDecoder(readBuffer)

	//decodeShards := make(map[int]KVShard)

	var decodeShards map[int]KVShard
	err = decoder.Decode(&decodeShards)
	if err != nil {
		t.Fatalf("decode decodeMemtable[%v] error[%v]", decodeShards, err)
	}
	if len(decodeShards) != len(kvShards) {
		t.Fatalf("kvShards[%v] != decodeShards[%v]", kvShards, decodeShards)
	}
	t.Logf("decodeShards[%v]", decodeShards)
	/*for key, value := range kvShards {
		if decodeValue, ok := decodeShards[key]; !ok || decodeValue != value {
			t.Fatalf("kvShards[%v] != decodeMemtable[%v]", kvShards, decodeMemtable)
		}
	}*/
}

type testInterface interface {
	getInt() int
}
type get0 struct {
}

func (*get0) getInt() int {
	return 0
}

type get1 struct {
}

func (*get1) getInt() int {
	return 1
}

/*
func TestEncodeAndDecodeInterface(t *testing.T) {
	var testGet testInterface
	testGet = &get0{}

	byteBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(byteBuffer)
	err := encoder.Encode(testGet)
	if err != nil {
		t.Fatalf("encode testInterface[%v] error", testGet)
	}

	memBytes := byteBuffer.Bytes()

	readBuffer := bytes.NewBuffer(memBytes)
	decoder := labgob.NewDecoder(readBuffer)

	var decodeTestInterface testInterface
	err = decoder.Decode(&decodeTestInterface)
	if err != nil {
		t.Fatalf("decode decodeTestInterface[%v] error[%v]", decodeTestInterface, err)
	}

}*/
