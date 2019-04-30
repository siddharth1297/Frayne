package kv

import (
	"errors"
)

// Coordinator

func (t *RpcKv) Coordinator(msg *KeyValueMessageHeader, reply *KeyValueMessageHeader) error {
	keyVal := GetMemberObject()

	switch msg.OptType {
	case PUT:
		return keyVal.coordinatorPut(msg, reply)
	case GET:
		return keyVal.coordinatorGet(msg, reply)
	case DELETE:
		return keyVal.coordinatorDelete(msg, reply)
	default:
		return errors.New("invalid operation")
	}
}
