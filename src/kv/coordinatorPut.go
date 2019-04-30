
















package kv

import (
	"fmt"
	"log"
	"member"
	"net/rpc"
	"parameter"
	"time"
)

func (keyVal *Kv) coordinatorPut(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader) error {
	log.Println("Coordinator PUT request: ", msg.EachEntry.Key)
	timeStamp := parameter.GetCurrentTimeStamp()

	var messages []KeyValueMessageHeader
	var targetNodes []member.Address
	var targetNodesString []string
	var hintingNodesString []string

	nextNodesStrings, _ := keyVal.c.GetN(msg.EachEntry.Key, ReplicationFactor+2) // 2: next 2 nodes for hinting

	if len(nextNodesStrings) < 3 {
		return keyVal.serverDownMessage(msg, reply)
	} else if len(nextNodesStrings) == 3 {
		targetNodesString = nextNodesStrings
		hintingNodesString = nil
	} else {
		targetNodesString = nextNodesStrings[0:3]
		hintingNodesString = nextNodesStrings[3:]
	}

	var hintingNodes []member.Address
	for _, v := range hintingNodesString {
		hintingNodes = append(hintingNodes, StringToAddress(v))
	}

	fmt.Println("Target Nodes string : ", targetNodesString)
	fmt.Println("Hinting Nodes string: ", hintingNodesString)

	mainNodeAddr := StringToAddress(targetNodesString[0])

	for i := 0; i < len(targetNodesString); i++ {
		storedAddress := StringToAddress(targetNodesString[i])
		targetNodes = append(targetNodes, storedAddress)
		var replicaType = SecondaryReplica
		if i == 0 {
			replicaType = PrimaryReplica
		}
		messages = append(messages, KeyValueMessageHeader{msg.OptType, Entry{msg.EachEntry.Key, msg.EachEntry.Val, timeStamp, keyVal.OwnAddress, mainNodeAddr, storedAddress, replicaType}})
	}

	var serverReplies = make([]KeyValueMessageHeader, len(targetNodes))
	var recvCall []*rpc.Call

	if len(targetNodes) < WriteQuorum {
		return keyVal.serverDownMessage(msg, reply)
	}
	var failureNodes []member.Address
	var sentTo []string //Holds address of the server, where rpc call is sent successfully
	var valuesGotFrom []string //Holds address of the server, which got successful rpc call return
	var quorum = 0 // Actual quorum: both sent and got
	var i int

	for i = 0; i < len(targetNodes); i++ {
		//fmt.Println("SERVER ADDRESS: ", targetNodesString[i])
		rpcClient, err := rpc.Dial("tcp", targetNodesString[i])
		if err != nil {
			log.Println("Error in dialing to server: ", targetNodesString[i], "\t: ", err)
			failureNodes = append(failureNodes, targetNodes[i])
			continue
		} else {
			//fmt.Println("Connected to: ", targetNodesString[i])
		}
		serverCall := rpcClient.Go("RpcKv.Server", &messages[i], &serverReplies[i], nil)
		//fmt.Println("SENT TO: ", messages[i].EachEntry.StoredReplicaAddress, "\t", i)
		recvCall = append(recvCall, serverCall)
		sentTo = append(sentTo, targetNodesString[i])
		//fmt.Println("Called to: ", targetNodesString[i])
	}

	if len(sentTo) < WriteQuorum {
		// Servers are down- not possible now

		//Undo the successful put at the server
		go keyVal.DeleteClient(msg.EachEntry.Key, sentTo)
		return keyVal.serverDownMessage(msg, reply)
	}

	var j int
	//Get from server
	for j = 0; j < len(sentTo); j++ {
		rCall := <-recvCall[j].Done

		if rCall.Error != nil {
			fmt.Println("Error in rpc receive: ", rCall.Error, rCall.Args.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress)
			failureNodes = append(failureNodes, rCall.Args.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress)
		} else {
			fmt.Println("Successful result: ", rCall.Reply.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress)
			recvCall = append(recvCall, rCall)
			valuesGotFrom = append(valuesGotFrom, AddressToString(rCall.Reply.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress))
			quorum++
			if quorum == ReplicationFactor {
				fmt.Println("---------Getting out---------: REPLICATION FACTOR MATCHED")
				return keyVal.successfulWriteMessage(msg, reply)
			} else if quorum == WriteQuorum {
				go keyVal.handleRest(j+1, len(sentTo), recvCall, messages[i-1], failureNodes, hintingNodes)
				fmt.Println("---------Getting out---------: WRITE QUORUM MATCHED")
				return keyVal.successfulWriteMessage(msg, reply)
			}
		}
	}
	if quorum >= WriteQuorum {
		// success nodes are stored in valuesGotFrom
		//success
		// return
		// sent to others
		return keyVal.successfulWriteMessage(msg, reply)
	} else {
		log.Println("///Undoing at : ", valuesGotFrom)
		//Undo the successful put at the server
		go keyVal.DeleteClient(msg.EachEntry.Key, valuesGotFrom)
	}

	// sent to other servers
	return keyVal.serverDownMessage(msg, reply)
}

func (keyVal *Kv) handleRest(i int, length int, recvCall []*rpc.Call, message KeyValueMessageHeader, failureNodes []member.Address, hintingNodes []member.Address) {
	//fmt.Println("////////////////////////////////////////")
	//fmt.Println("i: = ", i, "\tlen:= ", length)
	for j := i; j < length; j++ {
		rCall := <-recvCall[j].Done
		if rCall.Error != nil {
			fmt.Println("Error: ", rCall.Error)
			failureNodes = append(failureNodes, rCall.Args.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress)
		} else {
			fmt.Println("Successful result: ", rCall.Reply.(*KeyValueMessageHeader).EachEntry.StoredReplicaAddress)
		}
	}
	//fmt.Println("Failure Nodes: ", failureNodes)

	if len(failureNodes) == 0 {
		return
	}
	//Hinting: writeHintToServer()
	keyVal.writeHint(message.EachEntry, failureNodes, hintingNodes)
}

func (keyVal *Kv) writeHint(entry Entry, failureNodes []member.Address, hintingAddr []member.Address) {
	var hintEntry HintEntry
	hintEntry.EachEntry = entry
	hintEntry.ServerAddress = failureNodes
	if len(hintingAddr) == 0 {
		return
	}

	reply := HintedKvMessageReplyHeader{}
	var isCompleted = false
	var i = 0
	for ; !isCompleted; {
		address := AddressToString(hintingAddr[i])

		rpcClient, err := rpc.Dial("tcp", address)
		if err == nil {
			rpcErr := rpcClient.Call("RpcKv.WriteHintToHintServer", &hintEntry, &reply)
			if rpcErr != nil {
				fmt.Println("Error in writing Hint rpc return  ", rpcErr)
			} else {
				isCompleted = true
			}
		} else {
			fmt.Println("Error in writing Hint pinging to server: ", address, "\t:\t", err)
		}
		i = (i + 1) % len(hintingAddr)
		if i == 0 {
			time.Sleep(time.Duration(5) * time.Second) //sleep for 5 second
		}
	}
}
