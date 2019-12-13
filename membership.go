package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	Alias     string //vm1
	IP        string
	TimeStamp string
}

type Message struct {
	ID             string //ID = sender Node alias + _ + TimeStamp
	Type           string
	NodeID         Node   //nodeID = IP + TimeStamp
	MembershipList []Node // []NodeID
}

type Operation struct {
	Type           string // Add, Delete, AddAll, Read
	Member         Node
	MembershipList []Node
}

const (
	INTRO_IP = "172.22.152.206"
	PORT     = ":8000"
	TIMEOUT  = 3
)

var (
	index         int
	lossRate      float64
	localIP       string
	selfNode      Node
	timerList     [3]*time.Timer
	operation     = make(chan Operation) // channel for revise membership list
	memberInfo    = make(chan []Node)    // channel for read membership list
	receivedMsgID []string               //caches last 100 received message IDs
	timerMap      = make(map[string]int) // map for time, map[ip] timerNum
)

func runMembership() {
	rand.Seed(time.Now().UTC().UnixNano())

	//get stdin arguments
	var err error
	args := os.Args
	if len(args) > 1 {
		lossRate, err = strconv.ParseFloat(args[1], 32)
		errHandler(err, true, "Loss rate must be 0-1.")
	} else {
		lossRate = 0
	}

	//initialize file logging
	f, err := os.OpenFile("log.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {

		log.Fatal(err)
	}

	defer f.Close()
	log.SetOutput(f)
	localIP, err = getLocalIP()
	errHandler(err, true, "Main: get localIP failed")

	fmt.Println("My IP is " + localIP)

	genAliasMap(&aliasMap)

	selfNode = Node{
		Alias:     getAlias(localIP),
		IP:        localIP,
		TimeStamp: time.Now().Format(time.RFC850),
	}

	//init timer
	timerList[0] = time.NewTimer(time.Duration(TIMEOUT) * time.Second)
	timerList[1] = time.NewTimer(time.Duration(TIMEOUT) * time.Second)
	timerList[2] = time.NewTimer(time.Duration(TIMEOUT) * time.Second)
	timerList[0].Stop()
	timerList[1].Stop()
	timerList[2].Stop()
	for i := 0; i < 3; i++ {
		go monitorHeartbeat(i)
	}
	go membershipManage()
	go rcvMessages()
	go sendHeartbeats()

}

//send out a message to all nodes in nodeList
func sendMessages(msg Message, nodeList []Node) {
	for _, node := range nodeList {
		go sendMessage(msg, node)
	}
}

//send out a message to a Node
func sendMessage(msg Message, node Node) {
	jsonBytes := msg2json(msg)
	if len(jsonBytes) > 2048 {
		log.Println("Message size greater than 2048 bytes!")
	}
	conn, _ := connectUDP(node)

	defer conn.Close()
	//simulate packet loss
	if rand.Intn(100) <= int((1-lossRate)*100) {
		_, err := conn.Write(jsonBytes)
		errHandler(err, false, node.Alias+": UDP send Message failed.")
	}

	log.Println(msg.Type + " message sent from " + msg.NodeID.Alias + " to " + node.Alias)
	log.Println("Message size(bytes):  " + strconv.Itoa(len(jsonBytes)))
}

// listen and react to messages
func rcvMessages() Message {
	addr, err := net.ResolveUDPAddr("udp", PORT)
	errHandler(err, false, "listenHeartbeats: Resolve UDP Address failed.")
	conn, err := net.ListenUDP("udp", addr)
	errHandler(err, false, "listenHeartbeats: listen UDP failed.")
	defer conn.Close()

	//looping to catch messages
	for {
		rcvBuffer := make([]byte, 2048)

		read_len, addrUDP, err := conn.ReadFromUDP(rcvBuffer)

		errHandler(err, false, "Receiving message error from "+string(addrUDP.IP))
		msg := json2msg(rcvBuffer[:read_len])

		// checks for received messages
		if contains(receivedMsgID, msg.ID+msg.Type) {
			LogTime("message already received!")

			LogTime(msg.ID + msg.Type)
			continue
		} else {
			if len(receivedMsgID) < 100 {
				receivedMsgID = append(receivedMsgID, msg.ID+msg.Type)
			} else {
				receivedMsgID = append(receivedMsgID[1:], msg.ID+msg.Type)
			}
		}

		LogTime("Received" + msg.Type + "from" + msg.NodeID.Alias)

		switch msg.Type {
		case "Join":
			if localIP == INTRO_IP {
				operation <- Operation{
					"Add", msg.NodeID, msg.MembershipList}
				if len(AllFiles) != 0 {
					fmt.Println("update DHT for " + getAlias(msg.NodeID.IP))
					err := client.updateDHT(msg.NodeID)
					errHandler(err, false, "new node update DHT failed")
				}
				intro(msg)
				DHTJoinHandler(msg.NodeID)
			} else {
				if strings.Split(msg.ID, "_")[0] == INTRO_IP {
					if len(msg.MembershipList) == 0 {
						operation <- Operation{
							"Add", msg.NodeID, msg.MembershipList}
						DHTJoinHandler(msg.NodeID)
					} else {
						operation <- Operation{
							"AddAll", msg.NodeID, msg.MembershipList}
						// new node update
						initDHT()
						DHTJoinHandler(msg.NodeID)
					}
				}
			}
		case "Heartbeat":
			heartbeatReceived(msg)
		case "Failed", "Leave":
			printAndLogTime(msg.NodeID.Alias + " " + msg.Type + "!")
			operation <- Operation{
				"Delete", msg.NodeID, msg.MembershipList}
			//gossip msg
			gossipMessage(msg)
			//delete RPC conn
			client.Delete(msg.NodeID.IP)
			// update DHT data
			DHTFailedHandler(msg.NodeID)

			////reassign failed MapleJuice tasks
			if len(taskMonitor.TaskStatus) != 0 && !contains(receivedMsgID, msg.ID){
				failSignal <- msg.NodeID.IP
			}

		case "Intro":
			ack := genMessage("Ack", selfNode, []Node{})
			sendMessage(ack, msg.NodeID)
			operation <- Operation{
				"Add", msg.NodeID, msg.MembershipList}
			LogTime("Introducer rejoin message received!")
		case "Ack":
			operation <- Operation{
				"Add", msg.NodeID, msg.MembershipList}
			LogTime("Member Ack message received from " + msg.NodeID.Alias)
		}

	}
}

//reset corresponding timer from timerList by heartbeat
func heartbeatReceived(htbtMsg Message) {
	for key, value := range timerMap {
		if key == htbtMsg.NodeID.IP {
			LogTime("heartbeatReceived!")
			timerList[value].Reset(time.Second * TIMEOUT)
		}
	}
}

//start sending heartbeats to neighbours
func sendHeartbeats() {
	var htbtMsg Message

	for {
		htbtMsg = genMessage("Heartbeat", selfNode, nil)
		memList := getSendToNodes()
		if len(memList) > 0 {
			go sendMessages(htbtMsg, memList)
			LogTime("heartbeat sent to: ")
			log.Println(memList)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//monitor heartbeat for failure
func monitorHeartbeat(index int) {
	listenList := getListenToNodes(nil)
	if len(listenList) == 0 {
		timerList[index].Stop()
	}
	<-timerList[index].C
	LogTime("timer triggered!")
	listenToList := getListenToNodes(nil)
	log.Println("now listen to list is :")
	log.Println(listenToList)
	curIp := ""
	for key, value := range timerMap {
		if value == index {
			curIp = key
		}
	}
	if curIp != "" {
		var deleteNode Node
		for _, v := range listenToList {
			if curIp == v.IP {
				deleteNode = v
			}
		}
		operation <- Operation{
			Type:           "Delete",
			Member:         deleteNode,
			MembershipList: nil,
		}
		DHTFailedHandler(deleteNode)
		gossipMessage(genMessage("Failed", deleteNode, nil))
	} else {
		timerList[index].Stop()
	}

	monitorHeartbeat(index)
}

//gossip message to 3 targets
func gossipMessage(msg Message) {
	sendToList := getSendToNodes()
	sendMessages(msg, sendToList)
}

func intro(msg Message) {
	//var memList []Node
	operation <- Operation{
		"Read", Node{}, []Node{}}
	memList := <-memberInfo
	// send list to new Node
	introMsg := genMessage("Join", Node{}, memList)
	go sendMessage(introMsg, msg.NodeID)
	// send new Node info to all old members
	for _, v := range memList {
		if !isSameNode(msg.NodeID, v) && v.IP != INTRO_IP {
			introMsg := genMessage("Join", msg.NodeID, []Node{})
			go sendMessage(introMsg, v)
		}
	}
}

func membershipManage() {
	var membership []Node

	if localIP == INTRO_IP {
		addMember(&membership, selfNode)
		fmt.Println("before init")
		NodeHashKeys = append(NodeHashKeys, hash([]byte(localIP)))
		NodeHash2IP[hash([]byte(localIP))] = localIP
		MyMasterFiles = []int {}
		replicasNodes = []int {}
		MySDFSFiles = Files{}
		fmt.Println("after init")
	}

	for {
		select {
		case op := <-operation:
			if op.Type == "Add" {
				if !containSameNode(membership, op.Member) {
					addMember(&membership, op.Member)
					sortMemberList(&membership)
					updateTimerMapForJoin(membership)
				}
			} else if op.Type == "Delete" {
				if containSameNode(membership, op.Member) {
					deleteMember(&membership, op.Member)
					sortMemberList(&membership)
					updateTimerMapForDelete(membership, op.Member)
				}
			} else if op.Type == "Read" {
				memberInfo <- membership
			} else if op.Type == "Clear" {
				membership = []Node{}
			} else {
				membership = op.MembershipList
				sortMemberList(&membership)
				updateTimerMapForJoin(membership)
			}
		}
	}
}

func updateTimerMapForJoin(list []Node) {
	listenList := getListenToNodes(list)
	for i, v := range listenList {
		timerMap[v.IP] = i
	}
	resetAllTimers(TIMEOUT)
}
func updateTimerMapForDelete(list []Node, deleteNode Node) {
	listenList := getListenToNodes(list)
	index, ok := timerMap[deleteNode.IP]
	if ok {
		delete(timerMap, deleteNode.IP)
		for _, v := range listenList {
			if _, ok := timerMap[v.IP]; !ok {
				timerMap[v.IP] = index
				timerList[index].Reset(time.Second * TIMEOUT)
				break
			}
		}
	}
}

func sortMemberList(list *[]Node) {
	tempMap := make(map[string]Node)
	var keys []string
	for _, v := range *list {
		node, ok := tempMap[v.IP]
		if ok {
			if node.TimeStamp < v.TimeStamp {
				tempMap[v.IP] = v
			}
		} else {
			tempMap[v.IP] = v
			keys = append(keys, v.IP)
		}
	}
	sort.Strings(keys)
	// map to list
	var tempList []Node
	for i, v := range keys {
		tempList = append(tempList, tempMap[v])
		if v == localIP {
			index = i
		}
	}
	*list = tempList
}

// check whether list contains Node
func containSameNode(list []Node, node Node) bool {
	for _, v := range list {
		if isSameNode(node, v) {
			return true
		}
	}
	return false
}

//add a Node to membership list when joining
func addMember(list *[]Node, node Node) {
	if containSameNode(*list, node) || isEmptyNode(node) {
		return
	}
	*list = append(*list, node)
}

//delete a Node from membership list when leaving or failed
func deleteMember(list *[]Node, node Node) {
	for i, v := range *list {
		if isSameNode(v, node) {
			if i != len(*list)-1 {
				*list = append((*list)[:i], (*list)[i+1:]...)
			} else {
				*list = (*list)[:i]
			}
		}
	}
}

//compare if two nodes are the same
func isSameNode(nodeA Node, nodeB Node) bool {
	if nodeA.Alias == nodeB.Alias && nodeA.IP == nodeB.IP && nodeA.TimeStamp == nodeB.TimeStamp {
		return true
	}
	return false
}

//to see if a Node is empty
func isEmptyNode(node Node) bool {
	if node.Alias == "" && node.IP == "" && node.TimeStamp == "" {
		return true
	}
	return false
}

//helper func for getting a nodeList of send-to neighbours from membership list
func getSendToNodes() []Node {
	operation <- Operation{
		"Read", Node{}, []Node{}}
	memList := <-memberInfo
	log.Println(memList)
	var sendList []Node
	if len(memList) < 4 {
		for _, v := range memList {
			if !isSameNode(v, selfNode) {
				sendList = append(sendList, v)
			}
		}
		return sendList
	}

	return []Node{memList[(index+1)%len(memList)], memList[(index+2)%len(memList)], memList[(index+3)%len(memList)]}
}

//helper func for getting a nodeList of listen-to neighbours from membership list
func getListenToNodes(list []Node) []Node {
	var memList []Node
	if list == nil {
		operation <- Operation{
			"Read", Node{}, []Node{}}
		memList = <-memberInfo
	} else {
		memList = list
	}

	var listenList []Node
	if len(memList) < 4 {
		for _, v := range memList {
			if !isSameNode(v, selfNode) {
				listenList = append(listenList, v)
			}
		}
		return listenList
	}

	return []Node{memList[(index-3+len(memList))%len(memList)], memList[(index-2+len(memList))%len(memList)], memList[(index-1+len(memList))%len(memList)]}
}

//helper func for quickly generating a message
func genMessage(msgType string, node Node, nodeList []Node) Message {
	return Message{generateID(localIP), msgType, node, nodeList}
}

//reset all timers
func resetAllTimers(countdown int) {
	for _, timer := range timerList {
		go timer.Reset(time.Duration(countdown) * time.Second)
	}
}

//pretty print a Node
func printNode(node Node) {
	fmt.Println(node.Alias + ":" + node.IP + " " + node.TimeStamp)
}
