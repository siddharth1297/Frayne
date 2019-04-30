package member

import (
	"log"
	"os"
	"parameter"
	"time"
)

var TSuspicious uint64 = 10
var TFail uint64 = 10

func (mem *Member) NodeLoop() {
	var lastMsgTimestamp = parameter.GetCurrentTimeStamp()
	for ; ; {
		if mem.BFailed {
			log.Println(mem.Addr, "\tFailed ")
			os.Exit(0)
		}

		flag, msg := mem.checkMessage()

		if flag {
			// If message, then compare and update own membership list as per the message membership list
			mem.receiveCallBack(msg)
			lastMsgTimestamp = parameter.GetCurrentTimeStamp()
		} else {
			if parameter.GetCurrentTimeStamp()-lastMsgTimestamp >= 15 {
				//ts := parameter.GetCurrentTimeStamp()
				//fmt.Println(ts, " ", lastMsgTimestamp, " ", ts-lastMsgTimestamp)
				mem.updateOwnHeartBeatList()
				/*MembershipListMutex.Lock()
				fmt.Println("-----------")
				for i := 0; i < len(mem.MemberList); i++ {
					fmt.Println(mem.MemberList[i].Addr, "\t", mem.MemberList[i].HeartbeatCount, "\t", mem.MemberList[i].Timestamp, "\t", mem.MemberList[i].NodeState)
				}
				fmt.Println("-----------")
				MembershipListMutex.Unlock()*/
				lastMsgTimestamp = parameter.GetCurrentTimeStamp()
			}
		}
	}
}

/* RPC call Message for JOINREQ
 * Update own list by adding message member list
 * Returns: own list
 */
func (t *RpcMember) JoinRequest(message *MessageHeader, reply *MessageHeader) error {
	// Get Member object
	obj := GetMemberObject()

	// Call the call back function
	obj.receiveCallBack(*message)

	// Write to relay
	reply.MsgType = JOINREP
	MembershipListMutex.Lock()
	reply.MemberHeartbeatList = obj.MemberList
	MembershipListMutex.Unlock()
	return nil
}

/* RPC call Message for Heartbeat
 * Enqueue message to message list
 * Returns: nothing
 */
func (t *RpcMember) HeartBeatReq(message *MessageHeader, reply *MessageHeader) error {
	*reply = MessageHeader{}
	obj := GetMemberObject()
	// Enqueue message for avoid blocking of calling thread
	obj.Enqueue(*message)
	return nil
}

func (mem *Member) receiveCallBack(msg MessageHeader) {

	switch msg.MsgType {
	case JOINREQ:
		mem.updateGroup(msg.MemberHeartbeatList)
		mem.incrementMyHeartbeat()
		return
	case JOINREP:
		mem.InGroup = true
		mem.updateGroup(msg.MemberHeartbeatList)
		return
	case HEARTBEAT:
		mem.updateGroup(msg.MemberHeartbeatList)
		/*fmt.Println("~~~~~~~~~~~")
		for i := 0; i < len(mem.MemberList); i++ {
			fmt.Println(mem.MemberList[i].Addr, "\t", mem.MemberList[i].HeartbeatCount, "\t", mem.MemberList[i].Timestamp, "\t", mem.MemberList[i].NodeState)
		}
		fmt.Println("~~~~~~~~~~~")*/
		return
	}
}

func (mem *Member) NodeLoopOperations() {
	var initialValue = 0
	for ; ; {
		//time.Sleep(time.Duration(5) * time.Second) //sleep for 5 seconds
		time.Sleep(time.Duration(5) * time.Second) //sleep for 5 seconds

		mem.incrementMyHeartbeat()
		MembershipListMutex.Lock()
		memberListCopy := mem.MemberList
		MembershipListMutex.Unlock()
		// shuffle
		for i := initialValue; i < len(memberListCopy); i += 2 {
			if (memberListCopy[i].Addr.IpAdd.Equal(mem.Addr.IpAdd) && memberListCopy[i].Addr.Port == mem.Addr.Port) || memberListCopy[i].NodeState == DEAD {
				continue
			}
			//log.Println("Sending heartbeat to: ", memberListCopy[i])
			go func(ind int, memListCopy HeartbeatElements) {
				mem.SendMessage(HEARTBEAT, memListCopy[ind].Addr.IpAdd, memListCopy[ind].Addr.Port)
			}(i, memberListCopy)
		}
		if initialValue == 0 {
			initialValue = 1
		} else {
			initialValue = 0
		}
	}
}

func (mem *Member) updateGroup(msgHeartbeatList HeartbeatElements) {
	for i := 0; i < len(msgHeartbeatList); i++ {
		if !mem.updateNode(msgHeartbeatList[i]) {
			MembershipListMutex.Lock()
			mem.MemberList = append(mem.MemberList, msgHeartbeatList[i])
			MembershipListMutex.Unlock()
			log.Println(mem.Addr, " :\t", msgHeartbeatList[i].Addr, "\tJoined")
		}
	}
}

func (mem *Member) updateNode(msgMemberEntry HeartbeatElement) bool {
	timeStamp := parameter.GetCurrentTimeStamp()
	MembershipListMutex.Lock()
	for i := 0; i < len(mem.MemberList); i++ {
		if mem.MemberList[i].Addr.IpAdd.Equal(msgMemberEntry.Addr.IpAdd) && mem.MemberList[i].Addr.Port == msgMemberEntry.Addr.Port {

			if msgMemberEntry.HeartbeatCount > mem.MemberList[i].HeartbeatCount {
				mem.MemberList[i].HeartbeatCount = msgMemberEntry.HeartbeatCount
				mem.MemberList[i].Timestamp = timeStamp
				mem.MemberList[i].NodeState = ACTIVE
				mem.MemberList[i].NSChangeTimestamp = timeStamp
			} else if msgMemberEntry.HeartbeatCount == mem.MemberList[i].HeartbeatCount {
				if mem.MemberList[i].NodeState == ACTIVE && timeStamp-mem.MemberList[i].Timestamp >= TSuspicious {
					mem.MemberList[i].NodeState = SUSPICIOUS
					mem.MemberList[i].NSChangeTimestamp = timeStamp
					log.Println(mem.Addr, " :\t", mem.MemberList[i].Addr, "\tSuspicious")
				} else if mem.MemberList[i].NodeState == SUSPICIOUS && timeStamp-mem.MemberList[i].NSChangeTimestamp >= TFail {
					mem.MemberList[i].NodeState = DEAD
					mem.MemberList[i].NSChangeTimestamp = timeStamp
					log.Println(mem.Addr, " :\t", mem.MemberList[i].Addr, "\tDead")
				}
			} else if mem.MemberList[i].NodeState == DEAD && msgMemberEntry.NodeState == ACTIVE && msgMemberEntry.NSChangeTimestamp > mem.MemberList[i].NSChangeTimestamp {
				mem.MemberList[i].HeartbeatCount = msgMemberEntry.HeartbeatCount
				mem.MemberList[i].Timestamp = timeStamp
				mem.MemberList[i].NodeState = ACTIVE
				mem.MemberList[i].NSChangeTimestamp = timeStamp
				log.Println(mem.Addr, " :\t", msgMemberEntry.Addr, "\tReJoined")
			}
			MembershipListMutex.Unlock()
			return true
		}
	}
	MembershipListMutex.Unlock()
	return false
}

func (mem *Member) updateOwnHeartBeatList() {
	timeStamp := parameter.GetCurrentTimeStamp()
	MembershipListMutex.Lock()
	for i := 0; i < len(mem.MemberList); i++ {
		if mem.MemberList[i].Addr.IpAdd.Equal(mem.Addr.IpAdd) && mem.MemberList[i].Addr.Port == mem.Addr.Port {
			continue
		}
		if mem.MemberList[i].NodeState == ACTIVE && timeStamp-mem.MemberList[i].Timestamp >= TSuspicious {
			mem.MemberList[i].NodeState = SUSPICIOUS
			mem.MemberList[i].NSChangeTimestamp = timeStamp
			log.Println(mem.Addr, " :\t", mem.MemberList[i].Addr, "\t--Suspicious")
		} else if mem.MemberList[i].NodeState == SUSPICIOUS && timeStamp-mem.MemberList[i].NSChangeTimestamp >= TFail {
			mem.MemberList[i].NodeState = DEAD
			mem.MemberList[i].NSChangeTimestamp = timeStamp
			log.Println(mem.Addr, " :\t", mem.MemberList[i].Addr, "\t--Dead")
		}
	}
	MembershipListMutex.Unlock()
}

func (mem *Member) incrementMyHeartbeat() {
	mem.Heartbeat++
	timestamp := parameter.GetCurrentTimeStamp()

	MembershipListMutex.Lock()
	for i := 0; i < len(mem.MemberList); i++ {
		if mem.MemberList[i].Addr.IpAdd.Equal(mem.Addr.IpAdd) && mem.MemberList[i].Addr.Port == mem.Addr.Port {
			mem.MemberList[i].HeartbeatCount = mem.Heartbeat
			mem.MemberList[i].Timestamp = timestamp
			mem.MemberList[i].NodeState = ACTIVE
			mem.MemberList[i].NSChangeTimestamp = timestamp
			break
		}
	}
	MembershipListMutex.Unlock()
}
