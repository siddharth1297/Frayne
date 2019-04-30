package kv

import (
	"errors"
	"fmt"
	"log"
	"member"
)

func (t *RpcKv) Server(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader) error {
	keyVal := GetMemberObject()
	switch msg.OptType {
	case PUT:
		entry := msg.EachEntry
		insertErr := keyVal.insert(entry)
		if insertErr != nil {
			reply.EachEntry = entry
			return insertErr
		}
		reply.OptType = SUCCESS
		reply.EachEntry = msg.EachEntry
		log.Println("Putting <key, val>: ", reply.EachEntry.Key, "\t", reply.EachEntry.Val)
		return nil
	case GET:
		entry, exists := keyVal.CheckExistence(msg.EachEntry.Key)
		if exists {
			fmt.Println("Getting <key, val>: ", msg.EachEntry.Key, entry.Val)
			reply.EachEntry = entry
		} else {
			fmt.Println("Getting key not found <key>: ", msg.EachEntry.Key)
			return errors.New("key not found")
		}
		return nil
	case DELETE:
		entry, exists := keyVal.CheckExistence(msg.EachEntry.Key)
		if exists {
			log.Println("Deleting <key>: ", msg.EachEntry.Key)
			keyVal.delete(entry.Key)
			reply.EachEntry = entry
		} else {
			log.Println("Deleting <key>: ", msg.EachEntry.Key, "\t key not found")
			return errors.New("key not found")
		}
		return nil
	}
	return nil
}

func (keyVal *Kv) CheckExistence(key string) (Entry, bool) {
	//hashTableMutex.Lock()
	hashTableMutex.RLock()
	entry, exist := keyVal.HashTable[key]
	hashTableMutex.RUnlock()
	//hashTableMutex.Unlock()
	return entry, exist
}

func (KeyVal *Kv) insert(entry Entry) error {
	existingEntry, exists := KeyVal.CheckExistence(entry.Key)
	if exists && existingEntry.TimeStamp > entry.TimeStamp {
		log.Println("Error: not highest timestamp: ", existingEntry.Key)
		return errors.New("not highest timestamp")
	}

	hashTableMutex.Lock()
	KeyVal.HashTable[entry.Key] = entry
	hashTableMutex.Unlock()
	log.Println("inserted <key> : ", entry.Key)
	return nil
}

func (keyVal *Kv) delete(key string) {
	hashTableMutex.Lock()
	delete(keyVal.HashTable, key)
	hashTableMutex.Unlock()
}

func (keyVal *Kv) deleteTargetReplicaAndTargetNode(replicaType ReplicaType, address member.Address) {
	hashTableMutex.Lock()
	fmt.Print("Deleting keys: ")
	for _, entry := range keyVal.HashTable {
		if entry.Replica == replicaType && entry.PrimaryReplicaAddr.IpAdd.Equal(address.IpAdd) && entry.PrimaryReplicaAddr.Port == address.Port {
			delete(keyVal.HashTable, entry.Key)
			fmt.Print(entry.Key, " ")
		}
	}
	fmt.Println()
	hashTableMutex.Unlock()
}

func (keyVal *Kv) getKeyWithHashAndUpdate (hashValue uint32, nodeAddress member.Address) []Entry {
	var entries []Entry
	//hashTableMutex.Lock()
	hashTableMutex.RLock()
	for _, entry := range keyVal.HashTable {
		if entry.Replica == PrimaryReplica {
			keyHashValues := keyVal.c.GetHashKey(entry.Key)
			if keyHashValues < hashValue {
				entry.PrimaryReplicaAddr = keyVal.OwnAddress
				entry.Replica = SecondaryReplica
				entries = append(entries, entry)
			}
		}
	}
	//hashTableMutex.Unlock()
	hashTableMutex.RUnlock()
	log.Println("Entries for fetching from ", nodeAddress, "\t", entries)
	return entries
}

// change key's primary address to address
// returns keys, which are not present and it's ow address
func (keyVal *Kv) changeKeyPrimaryReplicaAddr(keys []string, address member.Address) ([]string, member.Address) {
	var absentKeys []string

	hashTableMutex.Lock()

	for _, key := range keys {
		// Don't use checkExistence as lock is not re entrant
		existingEntry, exists := keyVal.HashTable[key]
		if exists {
			existingEntry.PrimaryReplicaAddr = address
			keyVal.HashTable[key] = existingEntry
		} else {
			absentKeys = append(absentKeys, key)
		}
	}
	hashTableMutex.Unlock()
	return absentKeys, keyVal.OwnAddress
}