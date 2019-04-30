package kv

import (
	"container/list"
	"fmt"
	"log"
	"member"
	"net"
	"net/rpc"
	"os"
	"stathat.com/c/consistent"
	"strconv"
	"strings"
	"sync"
	"time"
)

var DEBUG = true

type KeyValueMessageHeader struct {
	OptType   OperationType
	EachEntry Entry
}

type HintedKvMessageHeader struct {
	Entries []HintEntry
}

type HintedKvMessageReplyHeader struct {
	OptType OperationType
	Index   int
}

var obj *Kv

type OperationType int
type RequestType int

type RpcKv KeyValueMessageHeader

const (
	PUT OperationType = iota
	GET
	DELETE
	SUCCESS
)

type ReplicaType int

const (
	PrimaryReplica ReplicaType = iota
	SecondaryReplica
)

var ReplicationFactor = 3
var ReadQuorum = 2
var WriteQuorum = 2

type Node struct {
	Addr         member.Address
	NodeHashCode uint64
}

type Entry struct {
	Key                  string
	Val                  string
	TimeStamp            uint64
	CoordinatorNode      member.Address
	PrimaryReplicaAddr   member.Address
	StoredReplicaAddress member.Address
	Replica              ReplicaType
}

type HintEntry struct {
	// kv Entry
	EachEntry Entry
	//
	ServerAddress []member.Address
}

type NodesVector []Node
type Nodes map[uint64]Node

type Kv struct {
	OwnAddress        member.Address
	MsgQueue          *list.List
	c                 *consistent.Consistent
	HashTable         map[string]Entry
	HintedReplicaList map[string][]HintEntry
}

//var hashTableMutex sync.Mutex
var hashTableMutex sync.RWMutex

func (KeyVal *Kv) InitKv(addr member.Address) {
	KeyVal.OwnAddress = addr
	KeyVal.MsgQueue = list.New()
	KeyVal.c = consistent.New()
	KeyVal.c.NumberOfReplicas = 3
	KeyVal.HashTable = make(map[string]Entry)
	KeyVal.HintedReplicaList = make(map[string][]HintEntry)

	kvRpc := new(RpcKv)
	err := rpc.Register(kvRpc)
	if err != nil {
		log.Println("Error in registering for rpc: ", err)
		os.Exit(1)
	}
	obj = KeyVal
}

func GetMemberObject() *Kv {
	return obj
}

func (KeyVal *Kv) FetchActiveNodes() int {
	memberList := member.GetMembershipList()
	keyValMemberList := KeyVal.c.Members()
	// Check whether new member added
	var m map[string]bool
	m = make(map[string]bool)
	for _, mem := range keyValMemberList {
		m[mem] = true
	}
	for _, mem := range memberList {

		_, exists := m[AddressToString(mem.Addr)]
		if !exists {
			if len(KeyVal.c.Members()) < 3 {
				KeyVal.c.Add(AddressToString(mem.Addr))
				//fmt.Println("1 New member found: ", mem.Addr)
				continue
			}
			//fmt.Println("2 New member found: ", mem.Addr)
			if mem.Addr.IpAdd.Equal(KeyVal.OwnAddress.IpAdd) && mem.Addr.Port == KeyVal.OwnAddress.Port {
				continue
			}
			//Add to ring and do stabilization
			affectedNodes := KeyVal.c.AddAndGetAffectedNode(AddressToString(KeyVal.OwnAddress), AddressToString(mem.Addr))
			fmt.Println("=========Affected Nodes : ", affectedNodes)
			for _, afn := range affectedNodes {
				if KeyVal.TransferReplicaToNewNodeClient(afn[0], afn[1]) {
					KeyVal.DeleteReplicaFromOldNodeClient(afn[0])
				}
			}
		}
	}
	//log.Println("--------------------STABILIZED--------------")
	return len(KeyVal.c.Members())

	// Check whether any member is dead
}

func (keyVal *Kv) StartKv() {
	ringLength := keyVal.FetchActiveNodes()
	for ; ringLength < 3;  {
		time.Sleep(time.Duration(3)*time.Second)
		ringLength = keyVal.FetchActiveNodes()
	}
	if ringLength == 3 {
		return
	}
	keyVal.c.Add(AddressToString(keyVal.OwnAddress))
	// Fetch data from other servers and handle key entry
	hashValues, nodes := keyVal.c.GetHashValues(AddressToString(keyVal.OwnAddress))
	log.Println("---length: ", len(hashValues))
	// Fetch data from other servers
	for i := 0; i < len(hashValues); i++ {
		log.Println("=====")
		log.Println(hashValues[i], "\t", nodes[i])
		keyVal.FetchReplicaToNewNodeClient(hashValues[i], nodes[i])
		log.Println("REPLICA FETCHED COMPLETED")
		// Change tertiary's
		nextNodes, _ := keyVal.c.GetNextNodes(hashValues[i]-1, 4)
		log.Println("Next Nodes: ", nextNodes)
		keyVal.ChangeReplicaPrimaryAddrClient(nextNodes[2], AddressToString(keyVal.OwnAddress))
		keyVal.DeleteReplicaClient(nextNodes[3])
		log.Println("||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
	}
	log.Println("||||||||||||||||||||||||||||COMPLETEDS||||||||||||||||||||||||||||||||||||")
}

// This function runs periodically for specified amount of time.
// First it checks and transfers hinted replicas.
// Then, it sleeps for specific duration of time.
// Then, checks if any new server is added to the system
func (keyVal *Kv) RecvLoop() {

	for ; ; {
		// Hinted replica checker
		keyVal.checkAndTransferReplica()

		// Sleep for some time
		time.Sleep(time.Duration(5) * time.Second) //sleep for 5 second
		keyVal.FetchActiveNodes()
	}
}

func StringToAddress(addressString string) member.Address {
	mainNodeAddrString := strings.Split(addressString, ":")
	ipString, portString := mainNodeAddrString[0], mainNodeAddrString[1]
	ip := net.ParseIP(ipString)
	port, _ := strconv.ParseUint(portString, 0, 64)
	return member.Address{IpAdd: ip, Port: port}
}

func AddressToString(address member.Address) string {
	addressString := address.IpAdd.String() + ":" + strconv.Itoa(int(address.Port))
	return addressString
}
