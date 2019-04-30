package main

import (
	"../kv"
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strings"
)

var addr string

func main() {
	var argc = len(os.Args)

	if argc != 2 {
		fmt.Println("Invalid Input")
		os.Exit(0)
	}
	addr = os.Args[1]
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatal("Cannot connect to the server", err)
	}

	for ; ; {
		var msg, reply kv.KeyValueMessageHeader
		fmt.Print("$ ")
		s := ScannerText()
		ss := strings.Fields(s)
		l := len(ss)
		if l == 0 {
			continue
		} else if l == 1 {
			fmt.Println("Invalid input")
			continue
		} else if l > 3 {
			fmt.Println("Error Input")
			continue
		}
		if ss[0] == "PUT" {
			if l == 2 {
				fmt.Println("Error Input")
				continue
			}
			msg.EachEntry.Key = ss[1]
			msg.EachEntry.Val = ss[2]
			msg.OptType = kv.PUT

			err := client.Call("RpcKv.Coordinator", msg, &reply)
			if err != nil {
				fmt.Println(err)
				continue
			}
			//fmt.Println(reply)
		} else if ss[0] == "GET" {
			if l == 3 {
				fmt.Println("Error Input")
				continue
			}
			msg.EachEntry.Key = ss[1]
			//msg.EachEntry.Val = ss[2]
			msg.OptType = 1

			err := client.Call("RpcKv.Coordinator", msg, &reply)
			if err != nil {
				fmt.Println(err)
				continue
			}
			//fmt.Println(reply.EachEntry)
			fmt.Println(reply.EachEntry.Val)

		} else if ss[0] == "DELETE" {
			if l != 2 {
				fmt.Println("Error Input")
				continue
			}
			msg.EachEntry.Key = ss[1]
			msg.OptType = kv.DELETE

			err := client.Call("RpcKv.Coordinator", msg, &reply)
			if err != nil {
				fmt.Println(err)
				continue
			}
			//fmt.Println("Successfully removed")

		} else {
			fmt.Println("Error Request")
		}

	}
}
func ScannerText() string {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	return scanner.Text()
}
