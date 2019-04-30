package kv

import (
	"log"
	"net/rpc"
	"time"
)

type DeleteKeys struct {
	Keys []string
}

func (keyVal *Kv) DeleteReplicaClient(nodeAddr string) {
	deleteKeysRequest := DeleteKeys{}
	response := ReplyS{}

	//hashTableMutex.Lock()
	hashTableMutex.RLock()
	for key := range keyVal.HashTable {
		deleteKeysRequest.Keys = append(deleteKeysRequest.Keys, key)
	}
	hashTableMutex.RUnlock()
	//hashTableMutex.Unlock()
	log.Println("---To be Deleted at ", nodeAddr, "\tkeys: ", deleteKeysRequest.Keys)
	for ; true; {

		// Send to the new Server
		rpcClient, err := rpc.Dial("tcp", nodeAddr)
		if err != nil {
			log.Println("Error in connection for deleting replica from old server: ", nodeAddr)
			return
		}

		rpcErr := rpcClient.Call("RpcKv.DeleteReplicaServer", &deleteKeysRequest, &response)
		if rpcErr != nil {
			log.Println("Error in deleting keys from older server ", nodeAddr, " : ", rpcErr)
		}

		if DEBUG {
			log.Println("1111Replica delete done at: ", nodeAddr)
		}

		if response.Response == SUCCESS {
			log.Println("111Replica delete successful at: ", nodeAddr)
			break
		}
		time.Sleep(time.Duration(10)*time.Second)
	}

}

// --- server ---
func (t *RpcKv) DeleteReplicaServer(request *DeleteKeys, response *ReplyS) error {
	keyVal := GetMemberObject()
	log.Println("Delete keys: ", request.Keys)
	for _, key := range request.Keys {
		keyVal.delete(key)
	}
	response.Response = SUCCESS
	return nil
}
