package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

//GrepRequest request info. for grep
type GrepRequest struct {
	Params []string
}

//result for grep
type GrepResult struct {
	ip     string
	result string
}

var (
	aliasMap = make(map[string]string)
)

//------------------------------Grep Function--------------------------------

// grep implementation
func grepLog(req GrepRequest) string {
	//run grep command on VM using os/exec
	path := "/home/**/" //file path of filename.log
	//path := "/Users/laolintou/Projects/go_projects/logfile/"
	cmdCode := ""
	for i := 0; i < len(req.Params); i++ {
		if i == len(req.Params)-1 {
			cmdCode = cmdCode + path + req.Params[i]
		} else if i == 1 {
			cmdCode = cmdCode + "-n " + req.Params[i] + " "
		} else {
			cmdCode = cmdCode + req.Params[i] + " "
		}
	}
	fmt.Println(cmdCode)
	command := exec.Command("/bin/sh", "-c", cmdCode)

	result, err := command.Output()
	if err != nil {
		log.Print("Executing command error:", err)
		return string("")
	}
	return string(result)
}

//handle grep call
func handleGrep() {
	var ipArray []string

	//Read nodes info. from file
	f, err := os.Open("server_list.txt")
	//f, err := os.Open("/Users/laolintou/Projects/github/ece428/logger/client/server_list.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		ipArray = append(ipArray, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	c := make(chan GrepResult, 5)

	fmt.Println("input grep command (grep [option] [regex] [filename]): ")

	newReq := getGrepReq()
	t1 := time.Now()

	for _, ip := range ipArray {
		go grepSingle(ip, newReq, c)
	}
	count := 0
	totalLines := 0
	for result := range c {
		count++
		line := strings.Count(result.result, "\n")
		totalLines += line
		if count == len(ipArray) {
			break
		}
	}
	fmt.Println("Total lines:", totalLines)
	totalTime := time.Since(t1)
	fmt.Println("Run Time:", totalTime)
}

//Compose grep command
func getGrepReq() GrepRequest {
	var newReq GrepRequest

	reader := bufio.NewReader(os.Stdin)
	textRaw, _ := reader.ReadString('\n')
	text := strings.Split(textRaw, " ")
	newReq.Params = text

	return newReq
}

//Grep from single server.
func grepSingle(ip string, req GrepRequest, c chan GrepResult) {
	var reply string

	client, err := rpc.Dial("tcp", ip)
	if err != nil {
		fmt.Println(GrepResult{ip, "Grep failed!"})
		c <- GrepResult{ip, "Grep failed!"}
		log.Print("dialing:", err)
		return
	}
	fmt.Println("tcp connection to ", ip, " established!")

	err = client.Call("Rpc.Grep", req, &reply)

	if err != nil {
		fmt.Println(GrepResult{ip, "Grep failed!"})
		c <- GrepResult{ip, "Grep failed!"}
		log.Print("Call error", err)
		_ = client.Close()
		return
	}

	//Handle case where grep result is empty
	if reply == "" {
		reply = "Found no match."
	}

	fmt.Println(GrepResult{ip, reply})
	c <- GrepResult{ip, reply}
	_ = client.Close()
}
//------------------------------General Function--------------------------------
func printAndLogTime(s string) {
	log.Println(s + time.Now().Format("2006-01-02 15:04:05.000000"))
	fmt.Println(s + time.Now().Format("2006-01-02 15:04:05.000000"))
}
func LogTime(s string) {
	log.Println(s + time.Now().Format("2006-01-02 15:04:05.000000"))
}
//marshal message
func msg2json(msg Message) []byte {
	var jsonData []byte
	jsonData, err := json.Marshal(msg)
	if err != nil {
		log.Println(err)
	}
	return jsonData
}

//unmarshal message
func json2msg(jsonData []byte) Message {
	var msg Message
	err := json.Unmarshal(jsonData, &msg)
	if err != nil {
		log.Println(err)
	}
	return msg
}

//establish UDP connection
func connectUDP(node Node) (net.Conn, error) {
	UDPAddr, err := net.ResolveUDPAddr("udp", node.IP+PORT)
	errHandler(err, false, node.Alias+": UDP resolve failed.")
	conn, err := net.DialUDP("udp", nil, UDPAddr)
	errHandler(err, false, node.Alias+": UDP dial failed.")

	if err != nil {
		return nil, err
	}
	return conn, err
}
func hash(data []byte) int {

	return int(sha1.Sum(data)[0]) * 256 + int(sha1.Sum(data)[1])
}
//log error and decide whether to exit program
func errHandler(err error, isFatal bool, errInfo ...string) {
	if err != nil {
		switch isFatal {
		case true:
			log.Fatal(err)
		case false:
			printAndLogTime(strings.Join(errInfo, " ") + err.Error())
		}
	}
}

//get IP of the local Node
func getLocalIP() (string, error) {
	addrList, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrList {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", errors.New("getLocalIP failed: No non-loopback local IP available")
}

func generateID(ip string) string {
	return ip + "_" + time.Now().Format(time.RFC850)
}



func genAliasMap(aliasMap *map[string]string) {
	(*aliasMap)["172.22.152.206"] = "vm1"
	(*aliasMap)["172.22.154.202"] = "vm2"
	(*aliasMap)["172.22.156.202"] = "vm3"
	(*aliasMap)["172.22.152.207"] = "vm4"
	(*aliasMap)["172.22.154.203"] = "vm5"
	(*aliasMap)["172.22.156.203"] = "vm6"
	(*aliasMap)["172.22.152.208"] = "vm7"
	(*aliasMap)["172.22.154.204"] = "vm8"
	(*aliasMap)["172.22.156.204"] = "vm9"
	(*aliasMap)["172.22.152.209"] = "vm10"
}

func getAlias(ip string) string {
	return aliasMap[ip]
}


//------------------------------Arrays Function--------------------------------
func indexOf(slice []int, value int) int {
	for k, v := range slice {
		if v == value {
			return k
		}
	}
	return -1
}

func removeValue(slice *[]int, value int) bool{
	flag := false
	for i, v := range *slice {
		if v == value {
			if i != len(*slice)-1 {
				*slice = append((*slice)[:i], (*slice)[i+1:]...)
			} else {
				*slice = (*slice)[:i]
			}
			flag = true
		}
	}
	return flag
}
func removeIndex(slice *[]int, idx int) {
	if idx != len(*slice)-1 {
		*slice = append((*slice)[:idx], (*slice)[idx+1:]...)
	} else {
		*slice = (*slice)[:idx]
	}
}

func addIndex(slice *[]int, index int, value int) {
	var newSlice []int
	if index == 0 {
		newSlice = []int{value}
		*slice = append(newSlice, (*slice)[0:]...)
	} else {
		newSlice = append((*slice)[:index], value)
		*slice = append(newSlice, (*slice)[index:]...)
	}
}
func removeFile(pFiles *Files, value int)  bool{
	files := *pFiles
	flag := false
	for i, v := range files {
		if v.FileHash == value {
			if i != len(files)-1 {
				files = append((files)[:i], (files)[i+1:]...)
			} else {
				files = (files)[:i]
			}
			flag = true
		}
	}
	*pFiles = files
	return flag
}

func getFileTimestamp(fileName string)  time.Time{
	var timestamp time.Time
	for _, v := range MySDFSFiles {
		if v.FileName == fileName {
			timestamp = v.TimeStamp
		}
	}
	return timestamp
}

// find the difference set of s1, s2
func diff(s1 []int, s2 []int) []int{
	j := 0
	i := 0
	var res []int
	sort.Ints(s1)
	sort.Ints(s2)
	for i<len(s1) && j < len(s2) {
		if s1[i] == s2[j] {
			i++
			j++
		} else if s1[i] < s2[j] {
			res = append(res, s1[i])
			i++
		} else {
			res = append(res, s2[j])
			j++
		}
	}
	if i == len(s1) && j < len(s2){
		res = append(res, s2[j:]...)
	}
	if i < len(s1) && j == len(s2) {
		res = append(res, s1[i:]...)
	}
	return res
}
/*
helper function for []string contains
from https://stackoverflow.com/questions/10485743/contains-method-for-a-slice/10486196
*/
func contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}
	_, ok := set[item]
	return ok
}
func findNodeIndexInList(node Node, nodeList []Node) int {
	for ix, candidateNode := range nodeList {
		if isSameNode(node, candidateNode) {
			return ix
		}
	}
	//log.Fatal("can't find Node in list")
	return 99999
}

//------------------------------FILE Function--------------------------------
func findFile(fileName string) {

}

//return a slice of filenames contained
func listLocalFiles() []string {
	var fileNames []string

	for _, file := range MySDFSFiles {
		fileNames = append(fileNames, file.FileName)
	}

	return fileNames
}

//This function copies a file from SDFS to local destination
func downloadLocal(fileName string, dstFileName string) {
	cpCmd := exec.Command("cp", "-rf", "./data/" + fileName, dstFileName)
	err := cpCmd.Run()
	if err != nil {
		printAndLogTime(err.Error())
	}
}

//This function copies a file from local destination to SDFS
func uploadLocal(fileName string, srcFileName string, dir2 string) error{
	cpCmd := exec.Command("cp", "-rf", srcFileName, "./data/" + fileName)
	err := cpCmd.Run()
	if err != nil {
		printAndLogTime(err.Error())
		return err
	}
	//change local file list
	removeFile(&MySDFSFiles, hash([]byte(fileName)))
	MySDFSFiles = append(MySDFSFiles, File{
		FileHash:  hash([]byte(fileName)),
		FileName:  fileName,
		TimeStamp: time.Now(),
		Dir2: dir2,
	})
	return nil
}
