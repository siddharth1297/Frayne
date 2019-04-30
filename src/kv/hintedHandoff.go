package kv

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

var HintedReplicaListMutex sync.RWMutex

// Write hint about the kv to server
func (t *RpcKv) WriteHintToHintServer(msg *HintEntry, reply *HintedKvMessageReplyHeader) error {
	keyVal := GetMemberObject()
	fmt.Println("-------\n", msg)
	HintedReplicaListMutex.Lock()
	for i := 0; i < len(msg.ServerAddress); i++ {
		serverAddr := msg.ServerAddress[i]
		serverAddrString := AddressToString(serverAddr)
		keyVal.HintedReplicaList[serverAddrString] = append(keyVal.HintedReplicaList[serverAddrString], *msg)
	}
	HintedReplicaListMutex.Unlock()
	fmt.Println("Hinting completed")
	reply.OptType = SUCCESS
	return nil
}


// This function runs periodically (Sleep duration can be specified).
// It checks for all the hinted kvs group by their nodes, where they are going to be stored.
// It tries to connect the server, each time it wakes up and if gets connected to the server, it sends all the kvs to it.
func (keyVal *Kv) checkAndTransferReplica() {

		HintedReplicaListMutex.RLock()
		for key := range keyVal.HintedReplicaList {
			// ping: if ping success send and remove to the address

			conn, err := net.Dial("tcp", key)
			if err != nil {
				fmt.Println("Error in Hinted Hand off pinging to server: ", key, "\t:\t", err)
			} else {
				fmt.Println("Hinted hand off Connected to: ", key)
				go keyVal.ReplicaTransferAndDelete(key, keyVal.HintedReplicaList[key])
				conn.Close()
			}
		}
		HintedReplicaListMutex.RUnlock()
}

// This function makes rpc call to the server and sends hints to the server
func (keyVal *Kv) ReplicaTransferAndDelete(address string, entries []HintEntry) {
	msg := HintedKvMessageHeader{entries}
	replay := HintedKvMessageReplyHeader{PUT, len(entries)}

	rpcClient, err := rpc.Dial("tcp", address)
	if err != nil {
		fmt.Println("Error in Hinted hand off dialing to server: ", address)
		return
	}
	fmt.Println("Hinted hand off Connected to: ", address)
	rpcErr := rpcClient.Call("RpcKv.PutHintedKV", &msg, &replay)
	if rpcErr != nil {
		fmt.Println("Error in hinted hand off rpc return  ", rpcErr)
		return
	}

	//Delete from Hinted hash table
	HintedReplicaListMutex.Lock()
	delete(keyVal.HintedReplicaList, address)
	HintedReplicaListMutex.Unlock()
}

// Write the hinted kvs to actual server it need to be stored, when server is up
func (t *RpcKv) PutHintedKV(msg *HintedKvMessageHeader, reply *HintedKvMessageReplyHeader) error {
	keyVal := GetMemberObject()

	var len = len(msg.Entries)
	for i := 0; i < len; i++ {
		entry := msg.Entries[i]
		entry.EachEntry.StoredReplicaAddress = keyVal.OwnAddress
		if keyVal.OwnAddress.IpAdd.Equal(entry.EachEntry.PrimaryReplicaAddr.IpAdd) && keyVal.OwnAddress.Port == entry.EachEntry.PrimaryReplicaAddr.Port {
			entry.EachEntry.Replica = PrimaryReplica
		} else {
			entry.EachEntry.Replica = SecondaryReplica
		}
		_ = keyVal.insert(entry.EachEntry)
	}
	reply.OptType = SUCCESS
	reply.Index = 0
	fmt.Println("Got hinting kvs---------------")

	return nil
}
