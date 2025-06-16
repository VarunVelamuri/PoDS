package writeaheadlog

import (
	"log"
)

type KVStore struct {
	hash map[string]string
	wal  *WriteAheadLog
}

func Init(path string) (*KVStore, error) {
	k := &KVStore{
		hash: make(map[string]string),
	}
	wal, exists, err := NewWriteAheadLog(path)
	if err != nil {
		return nil, err
	}

	if exists {
		data, err := wal.ReadFromLog()
		if err != nil {
			return nil, er
		}

		kvEntries, err := Deserialize(data)
		if err != nil {
			return nil, err
		}
		for _, kvEntry := range kvEntries {
			k.hash[kvEntry.key] = kvEntry.value
		}
	}
	return k, nil
}

func (kv *KVStore) Get(key string) string {
	return kv.hash[key]
}

func (kv *KVStore) appendLog(key, value string, entryType EntryType) error {
	kventry := NewKVEntry(key, value, entryType)
	data, err := kventry.Serialize()
	if err != nil {
		log.Printf("Error observed during KV entry serialize, err: %v", err)
		return err
	}

	err = kv.wal.AppendLog(data)
	if err != nil {
		return err
	}
	return nil
}

func (kv *KVStore) Put(key, value string) error {
	if err := kv.appendLog(key, value, UPSERT); err != nil {
		return err
	}
	kv.hash[key] = value
	return nil
}

func (kv *KVStore) Remove(key string) error {
	if err := kv.appendLog(key, "", DELETE); err != nil {
		return err
	}
	delete(kv.hash, key)
	return nil
}
