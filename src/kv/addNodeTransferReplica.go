package kv

import (
	"fmt"
	"log"
	"net/rpc"
)

type ReplicaTransferEntriesS struct {
	// kv Entry
	Entries []Entry
}

type ReplyS struct {
	Response OperationType
}

// B -> C -> D -> E ->F
// Insert X
// B -> C -> X -> D -> E ->F

// Add secondary replica of B to X and C to X

// Transfer replica to new node
// This function is called when a new server is added to the system. Then, the server holding the primary replica need to change the replicas
// Operated by the server holding the primary replica
// newNodeAddrString: newly added node's address in string type
// oldNodeAddrString: Address of replica stored, before the new server is joined to the system
// --- Client ---
func (keyVal *Kv) TransferReplicaToNewNodeClient(newNodeAddrString string, oldNodeAddrString string) bool {
	// Get all the primary Replica
	var replicaReqMessage ReplicaTransferEntriesS
	var replicaRepMessage ReplyS
	newNodeAddr := StringToAddress(newNodeAddrString)
	//hashTableMutex.Lock()
	hashTableMutex.RLock()
	for key := range keyVal.HashTable {
		// Copy all the primary replicas
		if keyVal.HashTable[key].Replica == PrimaryReplica {
			entry := keyVal.HashTable[key]
			// Change it's replica type
			entry.Replica = SecondaryReplica
			// Change it's address, where it is going to be stored
			entry.StoredReplicaAddress = newNodeAddr
			replicaReqMessage.Entries = append(replicaReqMessage.Entries, keyVal.HashTable[key])
		}
	}
	//hashTableMutex.Unlock()
	hashTableMutex.RUnlock()
	if DEBUG {
		log.Println("Replica copied. Target: ", newNodeAddrString, "\tlength = ", len(replicaReqMessage.Entries))
		fmt.Print("Keys: ")
		for _, v := range replicaReqMessage.Entries {
			fmt.Print(v.Key, " ")
		}
		fmt.Println()
	}
	// Send to the new Server
	rpcClient, err := rpc.Dial("tcp", newNodeAddrString)
	if err != nil {
		fmt.Println("Error in connection for transferring primary replica to newly added server: ", newNodeAddr)
		return false
	}

	rpcErr := rpcClient.Call("RpcKv.TransferReplicaToNewNodeServer", &replicaReqMessage, &replicaRepMessage)
	if rpcErr != nil {
		fmt.Println("Error in transferring primary replica to newly added server ", newNodeAddr, " : ", rpcErr)
		return false
	}
	if DEBUG {
		log.Println("Replica transfer successful at: ", newNodeAddr)
	}
	if replicaRepMessage.Response == SUCCESS {
		return true
	}

	return false
}

// --- Server ---
func (t *RpcKv) TransferReplicaToNewNodeServer(replicaReqMessage *ReplicaTransferEntriesS, replicaRepMessage *ReplyS) error {
	// Insert each entry to the hash table
	keyVal := GetMemberObject()
	for _, entry := range replicaReqMessage.Entries {
		_ = keyVal.insert(entry)
	}
	log.Println("Replica transfer successful")
	replicaRepMessage.Response = SUCCESS
	return nil
}
