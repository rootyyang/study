package kvraft

import (
	"bytes"
	"testing"

	"6.5840/labgob"
)

func TestDecode(t *testing.T) {
	memtable := make(map[string]string)
	memtable["test1"] = "value1"
	memtable["test2"] = "value2"

	byteBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(byteBuffer)
	err := encoder.Encode(memtable)
	if err != nil {
		t.Fatalf("encode memtable[%v] error", memtable)
	}

	memtableBytes := byteBuffer.Bytes()

	readBuffer := bytes.NewBuffer(memtableBytes)
	decoder := labgob.NewDecoder(readBuffer)

	decodeMemtable := make(map[string]string)
	err = decoder.Decode(&decodeMemtable)
	if err != nil {
		t.Fatalf("decode decodeMemtable[%v] error[%v]", decodeMemtable, err)
	}
	if len(decodeMemtable) != len(memtable) {
		t.Fatalf("Memtable[%v] != decodeMemtable[%v]", memtable, decodeMemtable)
	}
	for key, value := range memtable {
		if decodeValue, ok := decodeMemtable[key]; !ok || decodeValue != value {
			t.Fatalf("Memtable[%v] != decodeMemtable[%v]", memtable, decodeMemtable)
		}
	}
	//filterTable := make(map[UniqSeq]OpResult)
	//

}
