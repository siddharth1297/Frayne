package member

import (
	"container/list"
	"encoding/gob"
	"log"
	"net"
	"net/rpc"
	"os"
	"parameter"
	"strconv"
	"sync"
)

var obj *Member

// RPC type declaration
type RpcMember Member

// Mutex for Member message queue
var MemMessageQueueMutex sync.Mutex

// Mutex for Member member list
var MembershipListMutex sync.Mutex

type ActiveNodes []Address

type NodeState int

type Address struct {
	IpAdd net.IP
	Port  uint64
}

type HeartbeatElement struct {
	Addr              Address
	HeartbeatCount    uint64
	Timestamp         uint64
	NodeState         NodeState
	NSChangeTimestamp uint64
}

type HeartbeatElements []HeartbeatElement

type Member struct {
	Addr       Address
	Inited     bool
	BFailed    bool
	InGroup    bool
	Heartbeat  uint64
	MsgQueue   *list.List
	MemberList HeartbeatElements
}

const (
	ACTIVE NodeState = iota
	SUSPICIOUS
	DEAD
)

// Initialize node element
func (mem *Member) InitNode(addr net.IP, port uint64) {
	mem.Addr = Address{addr, port}
	mem.Inited = false
	mem.BFailed = false
	mem.InGroup = false
	mem.Heartbeat = 0
	mem.MsgQueue = list.New()
	MembershipListMutex.Lock()
	mem.MemberList = nil
	MembershipListMutex.Unlock()
}

func (mem Member) getPort() uint64 {
	return mem.Addr.Port
}

func GetMemberObject() *Member {
	return obj
}
func (mem *Member) initNode() {
	mem.BFailed = false
	mem.Inited = true
	mem.Heartbeat = 0
	mem.MsgQueue = list.New()
	timestamp := parameter.GetCurrentTimeStamp()
	mem.MemberList = append(mem.MemberList, HeartbeatElement{mem.Addr, mem.Heartbeat, timestamp, ACTIVE, timestamp})

	memRpc := new(RpcMember)
	err := rpc.Register(memRpc)
	if err != nil {
		log.Println("Error in registering for rpc: ", err)
		os.Exit(1)
	}
}

func (mem *Member) NodeStart(joinAdd net.IP, joinPort uint64, connectType string) {

	mem.initNode()

	createFlag := false
	var mutex sync.Mutex
	go mem.listenServer(&createFlag, &mutex)
	for ; ; {
		mutex.Lock()
		valueOfCreateFlag := createFlag
		mutex.Unlock()
		if valueOfCreateFlag {
			break
		}
	}

	isJoined := mem.introduceSelfToGroup(joinAdd, joinPort, connectType)
	if !isJoined {
		log.Println("Error in joining the group..  Please try latter")
		os.Exit(1)
	}
	obj = mem
}

func (mem Member) listenServer(createFlag *bool, createFlagMutex *sync.Mutex) {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(int(mem.getPort())))
	if err != nil {
		log.Println("Error in listening: ", err)
		os.Exit(1)
	}
	log.Println(mem.Addr, "\tListening... ")
	createFlagMutex.Lock()
	*createFlag = true
	createFlagMutex.Unlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(mem.Addr, "\tError in accepting request: ", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (mem Member) ReceiveAndDecode(conn net.Conn) {
	tmp := make([]byte, 0)
	_, err := conn.Read(tmp)
	if err != nil {
		log.Println(mem.Addr, "\tError in reading message: ", err)
		return
	}
	dec := gob.NewDecoder(conn)
	var msg MessageHeader
	er := dec.Decode(&msg)
	if er != nil {
		log.Println("decode error:", er)
		return
	}
	errClose := conn.Close()
	if errClose != nil {
		log.Println(mem.Addr, "\tError in closing connection: ", errClose)
	}

	mem.Enqueue(msg)
}

func (mem *Member) introduceSelfToGroup(joinAdd net.IP, joinPort uint64, connectType string) bool {
	if connectType == "S" {
		// Create Group
		mem.InGroup = true
		log.Println(mem.Addr, "\tStarting up group...: ", mem.Addr)
		return true
	} else if connectType == "J" {
		// Join Group
		log.Println(mem.Addr, "\tTrying to join group...: ", Address{joinAdd, joinPort})
		retMem, flag := mem.SendMessage(JOINREQ, joinAdd, joinPort)
		if !flag {
			log.Println("Error in joining group. Try later")
			return false
		} else {
			mem.receiveCallBack(retMem)
			return true
		}
	}
	return false
}

func GetMembershipList() []HeartbeatElement {
	memberObj := GetMemberObject()
	MembershipListMutex.Lock()
	memberList := memberObj.MemberList
	MembershipListMutex.Unlock()
	/*var activeNodes ActiveNodes
	for i := 0; i < len(memberList); i++ {
		if memberList[i].NodeState == ACTIVE {
			activeNodes = append(activeNodes, memberList[i].Addr)
		}
	}
	return activeNodes*/
	return  memberList
}
