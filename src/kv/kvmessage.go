package kv

import (
	"errors"
)

// key not found to client
func (keyVal *Kv)keyNotFoundMessage(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader) error {
	reply.EachEntry.Val = ""
	reply.EachEntry.Key = msg.EachEntry.Key
	reply.EachEntry.CoordinatorNode = keyVal.OwnAddress
	return errors.New("key not found")
}

// server down to client
func (keyVal *Kv)serverDownMessage(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader) error {
	reply.EachEntry.Val = ""
	reply.EachEntry.Key = msg.EachEntry.Key
	reply.EachEntry.CoordinatorNode = keyVal.OwnAddress
	return errors.New("server down, try after sometimes")
}

// write success
func (keyVal *Kv)successfulWriteMessage(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader) error {
	reply.OptType = SUCCESS
	reply.EachEntry = msg.EachEntry
	reply.EachEntry.CoordinatorNode = keyVal.OwnAddress
	// Can add timestamp
	//fmt.Println("---reply: \n", reply, "\n-----")
	return nil
}

// Read success
func (keyVal *Kv)successfulReadeMessage(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader, entry Entry) error {
	reply.OptType = SUCCESS
	reply.EachEntry = entry
	return nil
}