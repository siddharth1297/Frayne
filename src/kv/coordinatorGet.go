package kv

import (
	"errors"
	"fmt"
	"log"
	"net/rpc"
)

func (keyVal *Kv)coordinatorGet(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader) error {
	log.Println("Coordinator GET request: ", msg.EachEntry.Key)

	targetNodesStrings, _ := keyVal.c.GetN(msg.EachEntry.Key, ReplicationFactor)
	if len(targetNodesStrings) < ReadQuorum {
		return keyVal.serverDownMessage(msg, reply)
	}

	log.Println("Target Nodes for <", msg.EachEntry.Key, "> : ", targetNodesStrings)
	message := msg

	var serverReplays = make([]KeyValueMessageHeader, len(targetNodesStrings))
	var recvCall []*rpc.Call

	var sentTo []int //If rpc call sent successfully
	var valuesGotFrom []int
	var quorum = 0 // quorum count
	keyNotFoundCount := 0


	for i := 0; i < len(targetNodesStrings); i++ {
		rpcClient, err := rpc.Dial("tcp", targetNodesStrings[i])
		if err != nil {
			fmt.Println("Error in dialing to server: ", targetNodesStrings[i])
			continue
		} else {
			fmt.Println("Connected to: ", targetNodesStrings[i])
		}
		servCall := rpcClient.Go("RpcKv.Server", message, &serverReplays[i], nil)
		recvCall = append(recvCall, servCall)
		sentTo = append(sentTo, i)
	}

	if len(sentTo) < ReadQuorum {
		/*Servers are down- not possible now*/
		/*TRY later*/
		return keyVal.serverDownMessage(msg, reply)
	}

	//Get from server
	var j int
	for j = 0; j < len(sentTo); j++ {
		rCall := <-recvCall[j].Done

		if rCall.Error != nil {
			log.Println("Error: ", rCall.Error, rCall.Reply.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress)
			if rCall.Error.Error() == "key not found" {
				keyNotFoundCount++
				if keyNotFoundCount == ReplicationFactor {
					return errors.New("key not found")
				}
			}
		} else {
			log.Println("Successful result: ", rCall.Reply.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress)
			recvCall = append(recvCall, rCall)
			valuesGotFrom = append(valuesGotFrom, j)
			quorum++
			if quorum == ReadQuorum {
				// Get the highest timestamped entry
				ent := keyVal.getHighestEntryValue(recvCall)
				// Get return from other servers
				go keyVal.getFromOtherServers(j+1, targetNodesStrings, recvCall)
				// Sent success message to client
				return keyVal.successfulReadeMessage(msg, reply, ent)
			} else if quorum == ReplicationFactor {
				// Get the highest timestamped entry
				highestTimestampedEntry := keyVal.getHighestEntryValue(recvCall)
				//read repairS
				go keyVal.doReadRepair(recvCall, highestTimestampedEntry)
				// Sent success message to client
				return keyVal.successfulReadeMessage(msg, reply, highestTimestampedEntry)
			}
		}
	}
	if quorum >= ReadQuorum {
		ent := keyVal.getHighestEntryValue(recvCall)
		fmt.Println("--------Getting out-------3")
		return keyVal.successfulReadeMessage(msg, reply, ent)
	}

	// Sent Error message
	return keyVal.serverDownMessage(msg, reply)
}


func (keyVal *Kv) getFromOtherServers(i int, targetNodesStrings []string, recvCall []*rpc.Call) {
	fmt.Println("GET OTHER SERVER")
	length := len(targetNodesStrings)
	for j := i; j < length; j++ {
		rCall := <-recvCall[j].Done
		if rCall.Error == nil {
			// If no error, then add
			recvCall = append(recvCall, rCall)
			fmt.Println("--Successful result: ", rCall.Reply.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress)
		}
	}

	// Get highest entry value
	highestTimestampedEntry := keyVal.getHighestEntryValue(recvCall)

	// Read repair
	keyVal.doReadRepair(recvCall, highestTimestampedEntry)
	// If any targeted node, doesn't hold the value or failed, send it again
	var m map[string]bool
	m = make(map[string]bool)
	for _, r := range recvCall {
		entry := r.Reply.(*KeyValueMessageHeader)
		addrString := AddressToString(entry.EachEntry.StoredReplicaAddress)
		if addrString == "" {
			continue
		}
		m[addrString] = true
	}
	var newEntry = highestTimestampedEntry

	for _, node := range targetNodesStrings {
		_, exist := m[node]
		if !exist {
			// Send entry to it
			newEntry.Val = highestTimestampedEntry.Val
			newEntry.TimeStamp = highestTimestampedEntry.TimeStamp
			if AddressToString(highestTimestampedEntry.PrimaryReplicaAddr) == node {
				newEntry.Replica = PrimaryReplica
			} else {
				newEntry.Replica = SecondaryReplica
			}
			newEntry.StoredReplicaAddress = StringToAddress(node)
			rpcClient, err := rpc.Dial("tcp", node)
			if err != nil {
				log.Println("Not updated server - Error in dialing to server: ", node, "\t: ", err)
				continue
			} else {
				//fmt.Println("Not updated server - Connected to: ", node)
			}
			message := KeyValueMessageHeader{PUT, newEntry}
			reply := KeyValueMessageHeader{}

			servCall := rpcClient.Call("RpcKv.Server", &message, &reply)
			if servCall != nil && reply.OptType == SUCCESS {
				log.Println("Not updated server - Read repair at ", node, "\t successful")
			}
		}
	}

}

func (keyVal *Kv) getHighestEntryValue(recvCall []*rpc.Call) Entry {
	ent := recvCall[0].Reply.(*KeyValueMessageHeader)
	for i := 1; i < len(recvCall); i++ {
		newEnt := recvCall[i].Reply.(*KeyValueMessageHeader)
		if ent.EachEntry.TimeStamp < newEnt.EachEntry.TimeStamp {
			ent = newEnt
		}
	}
	return ent.EachEntry
}

func (keyVal *Kv) doReadRepair(recvCall []*rpc.Call, highestTimestampedEntry Entry) {
	// Read Repair
	for i := 1; i < len(recvCall); i++ {
		newEnt := recvCall[i].Reply.(*KeyValueMessageHeader)
		if newEnt.EachEntry.TimeStamp != highestTimestampedEntry.TimeStamp || newEnt.EachEntry.Val != highestTimestampedEntry.Val {
			newEnt.EachEntry.TimeStamp = highestTimestampedEntry.TimeStamp
			newEnt.EachEntry.Val = highestTimestampedEntry.Val
			message := KeyValueMessageHeader{PUT, newEnt.EachEntry}
			var reply = KeyValueMessageHeader{}
			addrString := AddressToString(newEnt.EachEntry.StoredReplicaAddress)

			rpcClient, err := rpc.Dial("tcp", addrString)
			if err != nil {
				log.Println("Read Repair - Error in dialing to server: ", addrString, "\t: ", err)
				continue
			} else {
				//fmt.Println("Read Repair - Connected to: ", addrString)
			}

			serverRet := rpcClient.Call("RpcKv.Server", &message, &reply)
			if serverRet != nil && reply.OptType == SUCCESS {
				log.Println("Read repaired at ", addrString, "\t successful")
			}
		}
	}
}
