package writeaheadlog

import (
	"bytes"
	"encoding/gob"
)

type EntryType int

// Only 2 types of values - UPSERT/DELETE
const (
	UPSERT EntryType = 0
	DELETE           = 1
)

type KVEntry struct {
	entryType EntryType
	key       string
	value     string
}

func NewKVEntry(key, value string, entryType EntryType) *KVEntry {
	return &KVEntry{
		key:       key,
		value:     value,
		entryType: entryType,
	}
}

func (k *KVEntry) Serialize() ([]byte, error) {

	// Create a new bytes.Buffer object
	buf := new(bytes.Buffer)
	// Create a gob encoder and encode the struct
	encoder := gob.NewEncoder(buf) // gob.NewEncoder takes an io.Writer

	err := encoder.Encode(k) // Encode the entire struct
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Deserialize(data []byte) ([]KVEntry, error) {

	// Create a new reader object for the encoded data
	reader := bytes.NewReader(data)

	// Create a gob decoder from the reader
	decoder := gob.NewDecoder(reader)

	// Create a new decoded KV Entry object
	decodedEntries := []KVEntry{}
	err := decoder.Decode(decodedEntries)
	if err != nil {
		return nil, err
	}
	return decodedEntries, nil

}
