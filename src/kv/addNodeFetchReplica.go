package kv

import (
	"log"
	"member"
	"net/rpc"
)

// B -> C -> D -> E ->F
// Insert X
// B -> C -> X -> D -> E ->F

type FetchAndModifyReplicaUsingHashS struct {
	HashValue          uint32
	PrimaryReplicaAddr member.Address
}

// X fetches all those key at D, whose hash lies (C, D]
// This function fetches all the relevant replicas from it's next server, when added to the ring
// Relevant refers all the key's whose hash lies in between the server and it's next server's hash
// --- client ---
func (keyVal *Kv) FetchReplicaToNewNodeClient(hashValue uint32, nextNodeAddrStr string) {
	request := FetchAndModifyReplicaUsingHashS{hashValue, keyVal.OwnAddress}
	response := ReplicaTransferEntriesS{}

	// Send to the new Server
	rpcClient, err := rpc.Dial("tcp", nextNodeAddrStr)
	if err != nil {
		log.Println("Error in connection for Fetching primary replica to newly added server: ", nextNodeAddrStr)
		return
	}

	rpcErr := rpcClient.Call("RpcKv.FetchReplicaToNewNodeServer", &request, &response)
	if rpcErr != nil {
		log.Println("Error in Fetching primary replica to newly added server ", nextNodeAddrStr, " : ", rpcErr)
	}

	if DEBUG {
		log.Println("Replica transfer successful at: ", AddressToString(keyVal.OwnAddress))
	}
	for _, entry := range response.Entries {
		entry.StoredReplicaAddress = keyVal.OwnAddress
		entry.Replica = PrimaryReplica
		entry.PrimaryReplicaAddr = keyVal.OwnAddress
		keyVal.insert(entry)
	}
	if DEBUG {
		log.Println("Replica insert successful at: ", AddressToString(keyVal.OwnAddress))
	}

}

// --- server ---
func (t *RpcKv) FetchReplicaToNewNodeServer(request *FetchAndModifyReplicaUsingHashS, response *ReplicaTransferEntriesS) error {
	keyVal := GetMemberObject()
	response.Entries = keyVal.getKeyWithHashAndUpdate(request.HashValue, request.PrimaryReplicaAddr)
	return nil

}
