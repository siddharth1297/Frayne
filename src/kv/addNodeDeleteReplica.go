package kv

import (
	"fmt"
	"log"
	"member"
	"net/rpc"
)

type ReplicaDeleteReqS struct {
	// Replica type need to be deleted
	DeleteReplicaType ReplicaType
	// Replica's primary replica address
	PrimaryReplicaAddr member.Address
}

// B -> C -> D -> E ->F
// Insert X
// B -> C -> X -> D -> E ->F

// Delete secondary replica of B from D and C from E

// Delete replica from node which is next to next of the main node
// This function is called when a new server is added to the system. Then, the server holding the primary replica need to change the replicas
// Operated by the server holding the primary replica
// oldNodeAddrString: Address of replica stored, before the new server is joined to the system
// --- Client ---
func (keyVal *Kv)DeleteReplicaFromOldNodeClient(nodeAddrString string) {
	// Delete from the older server
	var deleteReplicaReq ReplicaDeleteReqS
	var replicaRepMessage ReplyS

	deleteReplicaReq.DeleteReplicaType = SecondaryReplica
	deleteReplicaReq.PrimaryReplicaAddr = keyVal.OwnAddress

	// Continue until it deletes successfully
	for ; ; {

		rpcClient, err := rpc.Dial("tcp", nodeAddrString)
		if err != nil {
			fmt.Println("Error in connection for deleting replicas from older server: ", nodeAddrString)
			return
		}

		rpcErr := rpcClient.Call("RpcKv.DeleteReplicaFromOldNodeServer", &deleteReplicaReq, &replicaRepMessage)
		if rpcErr != nil {
			fmt.Println("Error in deleting replicas from older server ", nodeAddrString, " : ", rpcErr)
			return
		}

		if replicaRepMessage.Response == SUCCESS {
			if DEBUG {
				log.Println("Delete successful at: ", nodeAddrString)
			}
			return
		}
	}
}

// --- Server ---
func (t *RpcKv) DeleteReplicaFromOldNodeServer(replicaReqMessage *ReplicaDeleteReqS, replicaRepMessage *ReplyS) error {
	keyVal := GetMemberObject()
	keyVal.deleteTargetReplicaAndTargetNode(replicaReqMessage.DeleteReplicaType, replicaReqMessage.PrimaryReplicaAddr)
	if DEBUG {
		log.Println("Delete successful")
	}
	replicaRepMessage.Response = SUCCESS
	return nil
}