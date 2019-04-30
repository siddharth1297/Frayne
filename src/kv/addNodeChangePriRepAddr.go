package kv

import (
	"log"
	"member"
	"net/rpc"
)

type ChangeReplicaPrimaryRepAddrS struct {
	Keys               []string
	PrimaryReplicaAddr member.Address
}

type KeyNotPresentS struct {
	Keys        []string
	NodeAddress member.Address
}

// --- client ---
func (keyVal *Kv) ChangeReplicaPrimaryAddrClient(nodeAddrStr string, primaryReplicaAddr string) {
	request := ChangeReplicaPrimaryRepAddrS{}
	request.PrimaryReplicaAddr = keyVal.OwnAddress

	//hashTableMutex.Lock()
	hashTableMutex.RLock()
	for _, entry := range keyVal.HashTable {
		request.Keys = append(request.Keys, entry.Key)
	}
	//hashTableMutex.Unlock()
	hashTableMutex.RUnlock()
	response := KeyNotPresentS{}

	// Send to the new Server
	rpcClient, err := rpc.Dial("tcp", nodeAddrStr)
	if err != nil {
		log.Println("Error in connection for changing primary replica address : ", nodeAddrStr)
		return
	}
	log.Println("Connected to ", nodeAddrStr)
	rpcErr := rpcClient.Call("RpcKv.ChangeReplicaPrimaryAddrServer", &request, &response)
	if rpcErr != nil {
		log.Println("Error in changing primary replica address : ", nodeAddrStr, " : ", rpcErr)
		return
	}
	log.Println("")
	if DEBUG {
		log.Println("Replica primary replica address change successful at: ", nodeAddrStr)
	}

	log.Println("Failed keys : ", response.Keys, "\tAt : ", response.NodeAddress)
	// Send the absent keys and values to the server with new thread
	if len(response.Keys) != 0 {
		go keyVal.TransferAbsentReplicasClient(response.Keys, AddressToString(response.NodeAddress))
	}
}

func (keyVal *Kv) TransferAbsentReplicasClient(keys []string, nodeAddr string) {
	// Get all the primary Replica
	var replicaReqMessage ReplicaTransferEntriesS
	var replicaRepMessage ReplyS

	for _, key := range keys {
		entry, exists := keyVal.CheckExistence(key)
		if exists {
			entry.StoredReplicaAddress = StringToAddress(nodeAddr)
			entry.Replica = SecondaryReplica
			replicaReqMessage.Entries = append(replicaReqMessage.Entries, entry)
		}
	}
	// Send to the new Server
	rpcClient, err := rpc.Dial("tcp", nodeAddr)
	if err != nil {
		log.Println("Error in connection for transferring absent replicas to server: ", nodeAddr)
		return
	}

	rpcErr := rpcClient.Call("RpcKv.TransferReplicaToNewNodeServer", &replicaReqMessage, &replicaRepMessage)
	if rpcErr != nil {
		log.Println("Error in transferring absent replicas to  server ", nodeAddr, " : ", rpcErr)
		return
	}
	if DEBUG {
		log.Println("Absent Replica transfer successful at: ", nodeAddr)
	}
}

// --- server ---
func (t *RpcKv) ChangeReplicaPrimaryAddrServer(request *ChangeReplicaPrimaryRepAddrS, response *KeyNotPresentS) error {
	keyVal := GetMemberObject()
	response.Keys, response.NodeAddress = keyVal.changeKeyPrimaryReplicaAddr(request.Keys, request.PrimaryReplicaAddr)
	log.Println("FAILURE NODES: ", response.Keys)
	return nil
}
