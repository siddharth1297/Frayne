package member

import (
	"log"
	"net"
	"net/rpc"
	"strconv"
)

type MessageType int

const (
	JOINREQ MessageType = iota
	JOINREP
	HEARTBEAT
)

type MessageHeader struct {
	MsgType             MessageType
	MemberHeartbeatList HeartbeatElements
}

func (mem Member) SendMessage(msgType MessageType, recvAddr net.IP, recvPort uint64) (MessageHeader, bool) {
	// Check for message type and send message as per that
	// Declare message header
	var msg, replay MessageHeader
	msg.MsgType = msgType
	msg.MemberHeartbeatList = mem.MemberList

	serverAddress := recvAddr.String() + ":" + strconv.Itoa(int(recvPort))

	rpcClient, err := rpc.Dial("tcp", serverAddress)
	if err != nil {
		log.Println(mem.Addr, "\tError in RPC dialing:", err)
		return replay, false
	}

	switch msgType {
	case JOINREQ:
		err := rpcClient.Call("RpcMember.JoinRequest", msg , &replay)
		if err != nil {
			log.Println(mem.Addr, "\tERROR in sending JOINREQ message: ", err)
			return MessageHeader{}, false
		}
		break
	case HEARTBEAT:
		err := rpcClient.Call("RpcMember.HeartBeatReq", msg , &replay)
		if err != nil {
			log.Println(mem.Addr, "\tERROR in sending HEARTBEAT message: ", err)
			return MessageHeader{}, false
		}
		break
	}
	return replay, true
}

func (mem Member) Enqueue(msg MessageHeader) {
	MemMessageQueueMutex.Lock()
	mem.MsgQueue.PushBack(msg)
	MemMessageQueueMutex.Unlock()
}

func (mem Member) checkMessage() (bool, MessageHeader) {
	var msg MessageHeader
	var flag bool
	flag = false
	MemMessageQueueMutex.Lock()
	if mem.MsgQueue.Len() > 0 {
		m := mem.MsgQueue.Front()
		msg = m.Value.(MessageHeader)
		mem.MsgQueue.Remove(m)
		flag = true
	} else {
		msg = MessageHeader{}
		flag = false
	}
	MemMessageQueueMutex.Unlock()
	return flag, msg
}
