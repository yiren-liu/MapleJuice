package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	RpcPort      = ":2019"
	DownloadPort = ":4040"
)

type File struct {
	FileHash int
	FileName  string
	TimeStamp time.Time
	Dir2 string //secondary file directory for MapleJuice: "wikicorpus"
}

type Files []File
func (a *Files) Len() int           { return len(*a) }
func (a *Files) Swap(i, j int)      { (*a)[i], (*a)[j] = (*a)[j], (*a)[i] }
func (a *Files) Less(i, j int) bool { return (*a)[i].FileHash < (*a)[j].FileHash }
////get a map of all temp files belonging to each key on this node
//func (a Files) GetKeyTempFilesMap() map[string][]string {
//	var keyFilesMap = make(map[string][]string)
//
//	for _, file := range a {
//		s := strings.Split(file.FileName, "_")
//		if len(s) == 3 {
//			keyFilesMap[s[1]] = append(keyFilesMap[s[1]], file.FileName)
//		}
//	}
//
//	return keyFilesMap
//}
//get a map of all Intermediate files belonging to each key on this node
func (a *Files) GetKeyInterFilesMap() map[string][]string {
	var keyFilesMap = make(map[string][]string)

	for _, file := range *a {
		s := strings.Split(file.FileName, "_")
		if len(s) == 2 {
			keyFilesMap[s[1]] = append(keyFilesMap[s[1]], file.FileName)
		}
	}

	return keyFilesMap
}
func (a *Files) GetFileByName(fileName string) *File {
	var result *File

	for _, file := range *a {
		if file.FileName == fileName {
			result = &file
		}
	}

	return result
}
func (a *Files) GetFilesByDir2(dir2 string) []File {
	var result []File

	for _, file := range *a {
		if file.Dir2 == dir2 {
			result = append(result, file)
		}
	}

	return result
}



var (
	NodeHashKeys []int			// list for all nodes' hash Value
	NodeHash2IP  = make(map[int] string )      // list for all nodes' hash Value
	AllFiles     = make(map[int] string	)	// list for all files
	MyMasterFiles []int		// list for master files
	replicasNodes []int		// list for replicaNodes
	MySDFSFiles = Files{}		// list for store
	Rpc      *RPC
	client   *Client
	server   *Server
	mux      = &sync.Mutex{}
	reader   *bufio.Reader
	input 	 = make(chan bool)
	count    int64 = 0 // for dir upload
	//FileDirMap = make(map[string]string) //map of filename -> SDFS fileDir, i.e. "data1.txt"->"./data/test1/data1.txt"
)

func main() {
	Rpc = initRPC()
	client = initClient(selfNode)
	server = initServer(selfNode)
	clearFiles()
	runMembership()
	go server.listenAndServe(*Rpc)
	go taskFailureHandler()
	userInputHandler()
}

func clearFiles() {
	cmd := exec.Command("rm", "-rf", "./data")
	err := cmd.Run()
	errHandler(err, true, "error when delete files")
	cmd = exec.Command("mkdir", "data")
	err = cmd.Run()
	errHandler(err, true, "e")

	cmd = exec.Command("rm", "-rf", "./download")
	err = cmd.Run()
	errHandler(err, true, "error when delete files")
	cmd = exec.Command("mkdir", "download")
	err = cmd.Run()
	errHandler(err, true, "e")

	cmd = exec.Command("rm", "-rf", "./temp")
	err = cmd.Run()
	errHandler(err, true, "error when delete files")
	cmd = exec.Command("mkdir", "temp")
	err = cmd.Run()
	errHandler(err, true, "e")
}
//get function is used to get a file to dest from the SDFS locally or remotely
func get(fileName string, dstFileName string){
	//check all files
	mux.Lock()
	_, ok := AllFiles[hash([]byte(fileName))]
	if !ok {
		fmt.Println("SDFS file does not exist!")
		return
	}
	mux.Unlock()

	if contains(listLocalFiles(), fileName) {
		downloadLocal(fileName, dstFileName)
	} else {
		err := client.downloadFile(fileName, "./data/"+fileName, dstFileName, findFileMasterLoc(hash([]byte(fileName))), "")
		errHandler(err, false, "Error when getting file: "+fileName)
	}
}

func put(src string, fileName string, dir2 string) {
	//check local file
	f, err := os.Open(src)
	if err != nil{
		fmt.Println("Local file does not exist!")
		return
	}
	f.Close()


	ip := findFileMasterLoc(hash([]byte(fileName)))
	fmt.Println(NodeHashKeys)
	fmt.Println(NodeHash2IP)
	fmt.Println("master node is " + ip)
	fmt.Printf("file hash is %d \n" , hash([]byte(fileName)))
	if ip == selfNode.IP {
		DHTFilePutHandler(fileName, time.Now(), "", src, dir2)
	}else{
		err, res := client.putFile(fileName, time.Now(), selfNode.IP, ip, src, dir2)
		errHandler(err, false, "Error when putting file: "+fileName)
		if res {
			fmt.Println("Succeed!")
		} else {
			fmt.Println("Input operation cancelled")
		}
	}
}

func remove(fileName string) {
	//check all files
	mux.Lock()
	_, ok := AllFiles[hash([]byte(fileName))]
	if !ok {
		fmt.Println("SDFS file does not exist!")
		return
	}
	mux.Unlock()

	
	ip := findFileMasterLoc(hash([]byte(fileName)))
	if ip == selfNode.IP {
		DHTFileDeleteHandler(fileName)
	}else{
		err := client.removeFileAll(fileName, ip)
		errHandler(err, false, "Error when deleting file: " + fileName)
	}
}

func userInputHandler() {
	reader = bufio.NewReader(os.Stdin)
	for {
		fmt.Println("=============================")
		if selfNode.IP == INTRO_IP {
			fmt.Println("This is a introducer")
		}
		fmt.Println("1. list the membership list")
		fmt.Println("2. list self’s id")
		fmt.Println("3. join the group")
		fmt.Println("4. voluntarily leave the group")
		fmt.Println("5. grep command")
		fmt.Println("6. put localfilename sdfsfilename")
		fmt.Println("7. get sdfsfilename localfilename")
		fmt.Println("8. delete sdfsfilename")
		fmt.Println("9. store (list all SDFS files stored in this node)")
		fmt.Println("10. ls sdfsfilename (list all nodes storing this file)")
		fmt.Println("11. Exit")
		fmt.Println("12. List All Files")
		fmt.Println("13. put -r dir")
		fmt.Println("14. get SDFS file by dir")
		fmt.Println("15. run maple")
		fmt.Println("16. run juice")
		fmt.Println("Please Enter the Operation Number:")
		result, err := reader.ReadString('\n')
		input := strings.TrimSuffix(result, "\n")
		errHandler(err, false, "Read input failed")

		switch input {
		case "1":
			operation <- Operation{
				"Read", Node{}, []Node{}}
			memList := <- memberInfo
			for _, v := range memList {
				printNode(v)
			}
		case "2":
			printNode(selfNode)
		case "3":
			if selfNode.IP != INTRO_IP {
				join := genMessage("Join", selfNode, []Node{})
				sendMessage(join, Node{getAlias(INTRO_IP), INTRO_IP, ""})
			} else {
				//send Intro messages to all possible nodes.
				intro := genMessage("Intro", selfNode, []Node{})
				var nodeList []Node
				for ip := range aliasMap {
					if ip != selfNode.IP {
						nodeList = append(nodeList, Node{IP: ip})
					}
				}
				sendMessages(intro, nodeList)
				operation <- Operation{
					"Add", selfNode, []Node{}}
				LogTime("Introducer rejoin message sent!")
			}
		case "4":
			gossipMessage(genMessage("Leave", selfNode, []Node{}))
			operation <- Operation{
				"Clear", Node{}, []Node{}}
			fmt.Println("Node " + localIP + " leaving...")
			//os.Exit(0)
		case "5":
			handleGrep()
		case "6":

			fmt.Println("Please input: put localfilename sdfsfilename")
			result, err := reader.ReadString('\n')
			errHandler(err, false, "Read input failed")
			input := strings.TrimSuffix(result, "\n")
			params := strings.Split(input, " ")
			if len(params) != 3 || params[0] !=  "put"{
				fmt.Println("Invalid put command")
			} else {
				printAndLogTime("Starting timing!")
				start := time.Now()
				put(params[1], params[2], "")
				end := time.Now()
				printAndLogTime("Put took " + strconv.FormatFloat(end.Sub(start).Seconds(), 'f', 3, 64) +"sec.")
			}
		case "7":
			fmt.Println("Please input: get sdfsfilename localfilename")
			result, err := reader.ReadString('\n')
			errHandler(err, false, "Read input failed")
			input := strings.TrimSuffix(result, "\n")
			params := strings.Split(input, " ")
			if len(params) != 3 || params[0] !=  "get"{
				fmt.Println("Invalid get command")
			} else {
				printAndLogTime("Starting timing!")
				start := time.Now()
				get(params[1], params[2])
				end := time.Now()
				printAndLogTime("Get took " + strconv.FormatFloat(end.Sub(start).Seconds(), 'f', 3, 64) +"sec.")
			}
		case "8":
			fmt.Println("Please input: delete sdfsfilename")
			result, err := reader.ReadString('\n')
			errHandler(err, false, "Read input failed")
			input := strings.TrimSuffix(result, "\n")
			params := strings.Split(input, " ")
			if len(params) != 2 || params[0] != "delete"{
				fmt.Println("Invalid delete command")
			} else {
				printAndLogTime("Starting timing!")
				start := time.Now()
				remove(params[1])
				end := time.Now()
				printAndLogTime("Delete took " + strconv.FormatFloat(end.Sub(start).Seconds(), 'f', 3, 64) +"sec.")
			}
		case "9":
			fmt.Println("File Name: Timestamp")
			for _, file := range MySDFSFiles {
				fmt.Println(file.FileName + ": " + file.TimeStamp.Format("2006-01-02 15:04:05.000000"))
			}
		case "10":
			fmt.Println("Please input: ls sdfsfilename")
			result, err := reader.ReadString('\n')
			errHandler(err, false, "Read input failed")
			input := strings.TrimSuffix(result, "\n")
			params := strings.Split(input, " ")
			if len(params) != 2 || params[0] != "ls" {
				fmt.Println("Invalid ls command")
			} else {
				if _, ok := AllFiles[hash([]byte(params[1]))]; !ok {
					fmt.Println("File not in SDFS!")
					continue
				}
				index := 0
				for _, nodeHash := range NodeHashKeys {
					if hash([]byte(params[1])) > nodeHash {
						index++
					}
				}
				index = index % len(NodeHashKeys)
				for i := index; i < index + 4; i++{
					ip := NodeHash2IP[NodeHashKeys[i % len(NodeHashKeys)]]
					fmt.Println(getAlias(ip) + ": "+ ip)
				}
			}
		case "11":
			fmt.Println("Goodbye")
			fmt.Println(time.Now().Format("2006-01-02 15:04:05.000000"))
			os.Exit(0)
		case "12":
			fmt.Println(AllFiles)
		case "13":
			fmt.Println("Please input: put -r dir")
			result, err := reader.ReadString('\n')
			errHandler(err, false, "Read input failed")
			input := strings.TrimSuffix(result, "\n")
			params := strings.Split(input, " ")
			if len(params) != 3 || params[0] !=  "put"{
				fmt.Println("Invalid put command")
			} else {
				root := params[2]
				printAndLogTime("Starting timing!")
				start := time.Now()
				err := filepath.Walk(root, putFile)
				fmt.Printf("filepath.Walk() returned %v\n", err)
				end := time.Now()
				printAndLogTime("Upload took " + strconv.FormatFloat(end.Sub(start).Seconds(), 'f', 3, 64) +"sec.")
			}
		case "14":
			fmt.Println("Please input dir: ")
			result, err := reader.ReadString('\n')
			errHandler(err, false, "Read input failed")
			input := strings.TrimSuffix(result, "\n")
			params := strings.Split(input, " ")
			if len(params) != 1{
				fmt.Println("Invalid put command")
			} else {
				dir2 := params[0]
				files := GetFilesByDir2(client.getAllSDFSFiles(), dir2)
				if len(files) > 20{
					for i:=0;i<10;i++{
						fmt.Println(files[i].FileName)
					}
					fmt.Println("...")
					fmt.Println("total file number: ", len(files))
				}else{
					for _, file := range files{
						fmt.Println(file.FileName)
					}
				}
			}
		case "15":
			fmt.Println("Please input: maple mapleExe mapleNum interPrefix dataDir2")
			result, err := reader.ReadString('\n')
			errHandler(err, false, "Read input failed")
			input := strings.TrimSuffix(result, "\n")
			params := strings.Split(input, " ")
			if len(params) != 5{
				fmt.Println("Invalid put command")
			} else {
				mapleNum,_ := strconv.Atoi(params[2])
				Maple(params[1], mapleNum, params[3], params[4])
			}
		case "16":
			fmt.Println("Please input: juice mapleExe mapleNum interPrefix resultFileName deleteIntermediateFile")
			result, err := reader.ReadString('\n')
			errHandler(err, false, "Read input failed")
			input := strings.TrimSuffix(result, "\n")
			params := strings.Split(input, " ")
			if len(params) != 6{
				fmt.Println("Invalid put command")
			} else {
				mapleNum,_ := strconv.Atoi(params[2])
				Juice(params[1], mapleNum, params[3], params[4], params[5])
			}


		default:
			fmt.Println("Invalid command, try again!")
		}
	}
}

func putFile(path string, info os.FileInfo, err error) error {
	dir2 := strings.Split(path,"/")[0]
	if !info.IsDir(){
		printAndLogTime("Walking "+info.Name() + ": count " + strconv.Itoa(int(count)))
		put(path, info.Name(), dir2)
		count++
	}
	return nil
}



//------------------------------MASTER FUNCTIONS--------------------------------
// every Node init their DHT
func initDHT() {
	operation <- Operation{
		"Read", Node{}, []Node{}}
	memList := <-memberInfo
	fmt.Println(memList)
	for _, v := range memList {
		NodeHashKeys = append(NodeHashKeys, hash([]byte(v.IP)))
		NodeHash2IP[hash([]byte(v.IP))] = v.IP
	}
	sort.Ints(NodeHashKeys)
	fmt.Println(NodeHashKeys)
}

// update DHT after Node failed
func DHTFailedHandler(node Node) {
	mux.Lock()
	defer mux.Unlock()
	nodeHash := hash([]byte(node.IP))
	if indexOf(NodeHashKeys, nodeHash) == -1 {
		return
	}
	removeValue(&NodeHashKeys, nodeHash)
	delete(NodeHash2IP, nodeHash)

	newMasterFiles := findNewMasterFiles()
	idx := indexOf(NodeHashKeys, hash([]byte(selfNode.IP)))
	// when node is not a master of any file, do nothing
	if len(newMasterFiles) != 0 {
		if len(newMasterFiles) > len(MyMasterFiles) {
			// diffSet contains the files which you are the new master of
			diffSet := diff(newMasterFiles, MyMasterFiles)
			if len(replicasNodes) == 0 {
				replicasNodes = append(replicasNodes, NodeHashKeys[(idx+1)%len(NodeHashKeys)],
					NodeHashKeys[(idx+2)%len(NodeHashKeys)],NodeHashKeys[(idx+3)%len(NodeHashKeys)])
			}
			for _, fileHash := range diffSet {
				//send file to all 3 replica nodes
				fileName := AllFiles[fileHash]
				for _, nodeHash := range replicasNodes {
					timeStamp := getFileTimestamp(fileName)

					var fileDir2 string
					Pfile := MySDFSFiles.GetFileByName(fileName)
					//if file not at local, ask others for its info
					if Pfile == nil{
						AllFiles := client.getAllSDFSFiles()
						for _, file := range AllFiles{
							if file.FileName == fileName{
								fileDir2 = file.Dir2
								break
							}
						}
					}


					_ = client.uploadFile(fileName, timeStamp, "./data/", NodeHash2IP[nodeHash],
						fileDir2)
				}
			}
		} else {
			// send replicas to new replica node
			ok := removeValue(&replicasNodes, nodeHash)
			if  ok {
				replicasNodes = append(replicasNodes, NodeHashKeys[(idx+3)%len(NodeHashKeys)])
				for _, fileHash := range MyMasterFiles {
					// send file to the 3rd replica node
					fileName := AllFiles[fileHash]
					timeStamp := getFileTimestamp(fileName)

					var fileDir2 string
					Pfile := MySDFSFiles.GetFileByName(fileName)
					//if file not at local, ask others for its info
					if Pfile == nil{
						AllFiles := client.getAllSDFSFiles()
						for _, file := range AllFiles{
							if file.FileName == fileName{
								fileDir2 = file.Dir2
								break
							}
						}
					}


					_ = client.uploadFile(fileName, timeStamp, "./data/", NodeHash2IP[replicasNodes[2]],
						fileDir2)
				}
			}
		}
	}
	MyMasterFiles = newMasterFiles
}

// update DHT after new Node joined
func DHTJoinHandler(node Node) {
	mux.Lock()
	defer mux.Unlock()
	nodeHash := hash([]byte(node.IP))
	// other nodes update data
	if node.IP != "" && node.IP != selfNode.IP {
		NodeHashKeys = append(NodeHashKeys, nodeHash)
		sort.Ints(NodeHashKeys)
		NodeHash2IP[nodeHash] = node.IP
		fmt.Println(NodeHashKeys)
		fmt.Println(NodeHash2IP)
	}
	if len(AllFiles) == 0 {return}
	//fmt.Println("after DHTJoinHandler return")
	newMasterFiles := findNewMasterFiles()
	idx := indexOf(NodeHashKeys, hash([]byte(selfNode.IP)))
	// new master
	diffSet := diff(newMasterFiles, MyMasterFiles)
	if len(newMasterFiles) > len(MyMasterFiles) {
		// this node become some files' master
		if len(replicasNodes) == 0 {
			replicasNodes = append(replicasNodes, NodeHashKeys[(idx+1)%len(NodeHashKeys)],
				NodeHashKeys[(idx+2)%len(NodeHashKeys)],NodeHashKeys[(idx+3)%len(NodeHashKeys)])
		}
		for _, fileHash := range diffSet {
			//ask idx +1 to get file

			_ = client.downloadFile(AllFiles[fileHash], "./data/"+AllFiles[fileHash], "./data/"+AllFiles[fileHash], NodeHash2IP[NodeHashKeys[(idx + 1) % len(NodeHashKeys)]], "")
		}
		// old master
	} else if len(newMasterFiles) < len(MyMasterFiles) {
		// master position has been succeeded by other nodes， this node need to abdicate
		if len(newMasterFiles) == 0 {
			replicasNodes = []int{}
		}

	} else {
		// deal with
		if len(replicasNodes) != 0 && len(MyMasterFiles) != 0{
			newNodes := []int {NodeHashKeys[(idx+1)%len(NodeHashKeys)],
				NodeHashKeys[(idx+2)%len(NodeHashKeys)],NodeHashKeys[(idx+3)%len(NodeHashKeys)]}
			if indexOf(newNodes, nodeHash) > -1 {
				//  send all file in MyMasterFiles to new node
				for _, fileHash := range MyMasterFiles {
					fileName := AllFiles[fileHash]
					_ = client.uploadFile(fileName, getFileTimestamp(fileName), "./data/", node.IP,
						MySDFSFiles.GetFileByName(fileName).Dir2)
				}
				replicasNodes = newNodes
			}
		}
	}
	MyMasterFiles = newMasterFiles
	// gc
	if len(MySDFSFiles) != 0 {
		for _, file := range MySDFSFiles {
			masterIp := findFileMasterLoc(file.FileHash)
			if distanceTo(hash([]byte(masterIp))) > 3 {
				fmt.Println("delete SDFS")
				deleteFile(file.FileName)
				removeFile(&MySDFSFiles, file.FileHash)
			}
		}
	}

}

func DHTFileDeleteHandler(fileName string) {
	mux.Lock()
	defer mux.Unlock()
	fileHash := hash([]byte(fileName))
	if indexOf(MyMasterFiles, fileHash) > -1 {
		for _, nodeHash := range replicasNodes {
			// send delete to 3 replicas, try until succeed
			_ = client.removeFileAt(fileName, NodeHash2IP[nodeHash])
		}
		//delete local file
		err := deleteFile(fileName)
		if err != nil{
			errHandler(err, false, "Error when deleting file: " + fileName)
		}
		removeFile(&MySDFSFiles, fileHash)
		// send file delete msg to all node
		for _, nodeIP := range NodeHash2IP {
			if nodeIP != selfNode.IP{
				client.removeFileAll(fileName, nodeIP)
			}
		}
	}
	delete(AllFiles, fileHash)
}

//ipSrc -> 999.999.999.999:~/dir/filename
func DHTFilePutHandler(fileName string, timestamp time.Time, ipFrom string, src string, dir2 string) bool {
	//fmt.Println("DHTFilePutHandler param" + " " + fileName + " " + timestamp.Format(time.RFC850) + " " +
	//	ipFrom+" "+ src)
	mux.Lock()
	defer mux.Unlock()
	res := true
	fileHash := hash([]byte(fileName))
	isMaster := false
	isNewFile := false
	// master logic
	// update
	if indexOf(MyMasterFiles, fileHash) > -1  {
		isMaster = true
		//fmt.Println("I am master of " + fileName)
	} else if findFileMasterLoc(fileHash) == selfNode.IP {
		isMaster = true
		isNewFile = true
	}
	if isMaster {
		//if !isNewFile {
		//	preStamp := getFileTimestamp(fileName)
		//	if timestamp.Sub(preStamp).Seconds() < 60 {
		//		// call pop-up window
		//		err, ok := client.callPopWindow(ipFrom, fileName)
		//		errHandler(err, false, "call pop window function error")
		//		if !ok {
		//			// default no to overwrite
		//			res = false
		//			return res
		//		}
		//	}
		//}
		var err error
		if ipFrom != "" {
			//request file from source to this node
			//fmt.Println("call downloadFile")
			err = client.downloadFile(fileName, src, "./data/"+fileName, ipFrom, dir2)
			errHandler(err, false, "download source file failed")
		} else {
			//cp file from local
			//fmt.Println("upload from local...")
			err = uploadLocal(fileName, src, dir2)
			errHandler(err, false, "upload source file failed")

		}
		if err != nil{
			return false
		}

		// input new file
		if isNewFile {
			//fmt.Println("input new file")
			MyMasterFiles = append(MyMasterFiles, fileHash)
			if len(replicasNodes) == 0 {
				idx := indexOf(NodeHashKeys, hash([]byte(selfNode.IP)))
				replicasNodes = append(replicasNodes, NodeHashKeys[(idx+1)%len(NodeHashKeys)],
					NodeHashKeys[(idx+2)%len(NodeHashKeys)],NodeHashKeys[(idx+3)%len(NodeHashKeys)])
			}
		}
		fmt.Println(replicasNodes)
		for _, nodeHash := range replicasNodes {
			//fmt.Println("sending file to replica " + string(nodeHash))
			//send file to 3 replica nodes
			client.uploadFile(fileName, getFileTimestamp(fileName), "./data/", NodeHash2IP[nodeHash], dir2)
		}
		//send file put message to all nodes
		for _, nodeIP := range NodeHash2IP {
			if nodeIP != selfNode.IP{
				//fmt.Println("sending")
				client.putFile(fileName, getFileTimestamp(fileName), ipFrom, nodeIP, src, dir2)
			}
		}
	}

	AllFiles[fileHash] = fileName
	return res
}

// find the replicas location for a specific file hashval
func findFileMasterLoc(fileHashVal int) string {
	//mux.Lock()
	//defer mux.Unlock()
	index := 0
	for _, nodeHash := range NodeHashKeys {
		if fileHashVal > nodeHash {
			index++
		}
	}
	index = index % len(NodeHashKeys)
	return NodeHash2IP[NodeHashKeys[index]]
}

// recalculate all files to find files that this node masters
func findNewMasterFiles() []int{
	idx := indexOf(NodeHashKeys, hash([]byte(selfNode.IP)))
	var newMasterFiles []int
	for fileHash, _ := range AllFiles {
		if idx == 0 {
			if fileHash <= NodeHashKeys[idx] || fileHash > NodeHashKeys[len(NodeHashKeys)-1] {
				newMasterFiles = append(newMasterFiles, fileHash)
			}
		} else {
			if fileHash <= NodeHashKeys[idx] && fileHash > NodeHashKeys[idx-1] {
				newMasterFiles = append(newMasterFiles, fileHash)
			}
		}
	}
	return newMasterFiles
}

func distanceTo(destIp int) int{
	destId := indexOf(NodeHashKeys, destIp)
	selfId := indexOf(NodeHashKeys, hash([]byte(selfNode.IP)))
	if selfId < destId {
		selfId += len(NodeHashKeys)
	}
	return selfId - destId
}