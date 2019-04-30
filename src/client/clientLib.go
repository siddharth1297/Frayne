package main

import (
	"../kv"
	"fmt"
	"net/rpc"
	"strconv"
	"time"
)

type client struct {
	ip   string
	port int
	conn *rpc.Client
}

type ClientType client

func NewInit(ip string, port int) (ClientType, error) {
	var cli ClientType
	cli.ip = ip
	cli.port = port

	addr := ip + ":" + strconv.Itoa(port)
	fmt.Println("Address: ", addr)
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return cli, err
	}
	cli.conn = client

	return cli, nil
}

func (c ClientType)Put(key string, val string) error{
	var msg kv.KeyValueMessageHeader
	msg.EachEntry.Key = key
	msg.EachEntry.Val = val
	msg.OptType = kv.PUT

	_, err := c.callCoordinator(msg)
	if err != nil {
		return err
	}
	return nil
}

func (c ClientType)Get(key string) (string, string, error){
	var msg kv.KeyValueMessageHeader
	msg.EachEntry.Key = key
	msg.OptType = kv.GET

	reply, err := c.callCoordinator(msg)
	if err != nil {
		return "", "", err
	}
	return reply.EachEntry.Key, reply.EachEntry.Val, nil
}

func (c ClientType)Delete(key string, val string) error{
	var msg kv.KeyValueMessageHeader
	msg.EachEntry.Key = key
	msg.EachEntry.Val = val
	msg.OptType = kv.DELETE

	_, err := c.callCoordinator(msg)
	if err != nil {
		return err
	}
	return nil
}

func (c ClientType)callCoordinator(msg kv.KeyValueMessageHeader) (kv.KeyValueMessageHeader, error) {
	var reply kv.KeyValueMessageHeader
	err := c.conn.Call("RpcKv.Coordinator", msg, &reply)
	if err != nil {
		return reply, err
	}
	return reply, nil
}


func main() {

	client, err := NewInit("127.0.0.1", 8080)

	if err != nil {
		fmt.Println("Error in connecting to the coordinator: ", err)
		return
	}
	err = client.Put("A", "5")
	if err != nil {
		fmt.Println("Error in put: ", err)
	} else {
		fmt.Println("Put successful: A 5")
	}

	time.Sleep(time.Duration(5)*time.Second)

	key, val, getErr := client.Get("A")
	if getErr != nil {
		fmt.Println("Error in Get: ", err)
	} else {
		fmt.Println("Get success: ", key, "\t", val)
	}
}
