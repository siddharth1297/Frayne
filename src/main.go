package main

import (
	"errors"
	"fmt"
	"kv"
	"log"
	"member"
	"net"
	"os"
	"strconv"
)

func main() {
	/* pass
	 *  S ip port - 4 						$ go run main.go S 127.0.0.1 8080
	 *  J ip port join-ip join-port - 6  	$ go run main.go J 127.0.0.1 8081 127.0.0.1 8080
	 */
	var argc = len(os.Args)
	var joinAddr net.IP
	var joinPort uint64

	var mem member.Member

	if argc != 4 && argc != 6 {
		fmt.Println(errors.New("invalid arguments"))
		fmt.Println("for leader : S <ip> <port>")
		fmt.Println("for slave  : J <ip> <port> <join-ip> <join-port>")
		os.Exit(1)
	} else {

		// Parse ip
		addr := net.ParseIP(os.Args[2])
		if addr == nil {
			log.Println("Error in conversion of own ip from string to ip type")
			return
		}
		// Parse port
		port, err := strconv.ParseUint(os.Args[3], 0, 64)
		if err != nil {
			log.Println("Error in conversion of own port from string to number: ", err)
			return
		}

		if argc == 6 {
			// Parse join ip
			joinAddr = net.ParseIP(os.Args[4])
			if joinAddr == nil {
				log.Println("Error in conversion of join ip from string to ip type")
				return
			}
			// Parse join port
			joinPort, err = strconv.ParseUint(os.Args[5], 0, 64)
			if err != nil {
				log.Println("Error in conversion of join port from string to number: ", err)
				return
			}
		}

		// Initialize new node
		mem.InitNode(addr, port)
		log.Println(mem.Addr, "\tInitialized")
	}

	if os.Args[1] != "S" && os.Args[1] != "J" {
		log.Println("Specify whether to start: S or join: J ", os.Args[1])
		return
	}
	mem.NodeStart(joinAddr, joinPort, os.Args[1])
	log.Println(mem.Addr, "\tStarted")

	go mem.NodeLoopOperations()
	go mem.NodeLoop()

	var kv kv.Kv
	kv.InitKv(mem.Addr)
	kv.StartKv()

	log.Println(mem.Addr, "\tkv started...")
	kv.RecvLoop()
}
