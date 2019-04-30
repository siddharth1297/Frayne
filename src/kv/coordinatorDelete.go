package kv

import (
	"log"
	"net/rpc"
	"time"
)

func (keyVal *Kv) coordinatorDelete(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader) error {
	log.Println("Coordinator DELETE request: ", msg.EachEntry.Key)

	reply.OptType = SUCCESS
	reply.EachEntry = msg.EachEntry
	targetNodesStrings, _ := keyVal.c.GetN(msg.EachEntry.Key, ReplicationFactor)
	log.Println("Target Nodes for <", msg.EachEntry.Key, "> : ", targetNodesStrings)
	go keyVal.DeleteClient(msg.EachEntry.Key, targetNodesStrings)
	return nil
}

func (keyVal *Kv) DeleteClient(key string, targetNodesStrings []string) {
	var msg KeyValueMessageHeader
	var reply KeyValueMessageHeader
	msg.EachEntry.Key = key
	msg.OptType = DELETE

	for i, node := range targetNodesStrings {
		go func(node string) {
			for ; true;  {
				rpcClient, err := rpc.Dial("tcp", node)
				if err != nil {
					log.Println("Error in dialing to server: ", node)
				} else {
					log.Println("Connected to: ", node)
					retValue := rpcClient.Call("RpcKv.Server", msg, &reply)
					if retValue != nil {
						if retValue.Error() == "key not found" {
							log.Println("Successfully deleted at ", node, "\tList: ", targetNodesStrings)
							break
						}
					} else {
						if reply.OptType == SUCCESS {
							targetNodesStrings = append(targetNodesStrings[:i], targetNodesStrings[i+1:]...)
							log.Println("Successfully deleted at ", node, "\tList: ", targetNodesStrings)
							break
						}
					}
				}
				time.Sleep(time.Duration(10)*time.Second)
			}


		}(node)
	}
}
