package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"plugin"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	JUICE_PARTITION = "range"
)

//struct used to wrap filenames and properties of each task to be assigned to worker
type Task struct {
	TaskID      string
	FileNames   []string
	TaskType    string //maple, juice, merge .....
	InterPrefix string //sdfs_intermediate_filename_prefix
	TaskExe     string //filename of taskexe file
}

//RPC response struct to master node
type TaskStat struct {
	TaskID     string
	NodeIP     string
	taskStatus string
}

//task monitor map for master
type TaskMap struct {
	Worker2Task   map[string][]string             //master has a map that monitors all ongoing tasks at each node: workerIP -> []taskIDs
	Task2Worker   map[string]string               //master has a map of each task's worker node: taskID -> workerIP
	TaskStatus    map[string]bool                 //map of taskID -> completed? false/true
	TaskID2Task   map[string]*Task                //map of taskID -> task (used for reassigning tasks)
	TaskID2KeyMap map[string]*map[string][]string //map of taskID -> pointer of keyMap
}

func (m *TaskMap) addTask(task Task, nodeIP string) {
	m.Worker2Task[nodeIP] = append(m.Worker2Task[nodeIP], task.TaskID)
	m.Task2Worker[task.TaskID] = nodeIP
	m.TaskStatus[task.TaskID] = false
	m.TaskID2Task[task.TaskID] = &task
}
func (m *TaskMap) deleteTask(task Task) {
	nodeIP, hasTask := m.Task2Worker[task.TaskID]
	if hasTask {
		m.Worker2Task[nodeIP] = removeStringFromSlice(m.Worker2Task[nodeIP], task.TaskID)
		delete(m.Task2Worker, task.TaskID)
		delete(m.TaskStatus, task.TaskID)
		delete(m.TaskID2Task, task.TaskID)
		delete(m.TaskID2KeyMap, task.TaskID)
	}
}

//delete node and all tasks in this node
func (m *TaskMap) deleteNode(nodeIP string) {
	tasks, hasNode := m.Worker2Task[nodeIP]
	if hasNode {
		delete(m.Worker2Task, nodeIP)
		for _, taskID := range tasks {
			delete(m.Task2Worker, taskID)
			delete(m.TaskStatus, taskID)
			delete(m.TaskID2Task, taskID)
			delete(m.TaskID2KeyMap, taskID)
		}
	}
}
func (m *TaskMap) addKeyMap(taskID string, PKeyMap *map[string][]string) {
	m.TaskID2KeyMap[taskID] = PKeyMap
}
func (m *TaskMap) reset() {
	m.Worker2Task = make(map[string][]string)
	m.Task2Worker = make(map[string]string)
	m.TaskStatus = make(map[string]bool)
	m.TaskID2Task = make(map[string]*Task)
	m.TaskID2Task = make(map[string]*Task)
}
func (m *TaskMap) clearAll() {
	m.Worker2Task = make(map[string][]string)
	m.Task2Worker = make(map[string]string)
	m.TaskStatus = make(map[string]bool)
	m.TaskID2Task = make(map[string]*Task)
	m.TaskID2Task = make(map[string]*Task)
	m.TaskID2KeyMap = make(map[string]*map[string][]string)
}

//RPC response struct to master node
type TaskRes struct {
	TaskID     string
	taskStatus string
	TaskKeyMap map[string][]string //maple result return
}

var (
	taskCount      int //used to increment taskID
	taskStatusRecv = make(chan TaskStat)
	taskMonitor    = TaskMap{
		Worker2Task:   make(map[string][]string),
		Task2Worker:   make(map[string]string),
		TaskStatus:    make(map[string]bool),
		TaskID2Task:   make(map[string]*Task),
		TaskID2KeyMap: make(map[string]*map[string][]string),
	}
	failSignal     = make(chan string, 100) //nodeID
	completeSignal = make(chan string)     //

)

//main logic
func mapleJuice(mapleExe string, mapleNum int, interPrefix string, dataDir2 string, deleteInter string) {

	//0. put files into SDFS
	//...taskCount

	if len(getWorkerIPs()) > mapleNum {
		printAndLogTime("Error input: maple number must be larger than living workers!")
		return
	}

	//1. maple phase
	taskMonitor.reset()
	taskCount = 0
	files := GetFilesByDir2(client.getAllSDFSFiles(), dataDir2)

	go taskFailureHandler()
	partitionTask(mapleExe, "maple", files, interPrefix)

	<-completeSignal
	partitionMergeResult(interPrefix)
	taskMonitor.clearAll()
	taskCount = 0
	printAndLogTime("maple complete!")
	//2. juice phase
	files = GetFilesByDir2(client.getAllSDFSFiles(), interPrefix)
	partitionTask(mapleExe, "juice", files, interPrefix)

}

func Maple(mapleExe string, mapleNum int, interPrefix string, dataDir2 string) {

	if len(getWorkerIPs()) > mapleNum {
		printAndLogTime("Error input: maple number must be larger than living workers!")
		return
	}

	//maple phase
	taskMonitor.reset()
	//files := MySDFSFiles.GetFilesByDir2(dataDir2)
	files := GetFilesByDir2(client.getAllSDFSFiles(), dataDir2)
	fmt.Println(files)
	partitionTask(mapleExe, "maple", files, interPrefix)
	<-completeSignal
	partitionMergeResult(interPrefix)
	taskMonitor.clearAll()
	taskCount = 0
	printAndLogTime("maple complete!")
}

func Juice(juiceExe string, mapleNum int, interPrefix string, dstFileName string, deleteInter string) {

	if len(getWorkerIPs()) > mapleNum {
		printAndLogTime("Error input: maple number must be larger than living workers!")
		return
	}

	//juice phase
	taskMonitor.reset()
	//files := MySDFSFiles.GetFilesByDir2(interPrefix)
	files := GetFilesByDir2(client.getAllSDFSFiles(), interPrefix)
	partitionTask(juiceExe, "juice", files, interPrefix)
	<-completeSignal
	mergeJuice(dstFileName)
	taskMonitor.clearAll()
	taskCount = 0

	//after juice task complete, check if needs to delete intermediate files
	if deleteInter == "1" {
		for _, File := range GetFilesByDir2(client.getAllSDFSFiles(), interPrefix) {
			remove(File.FileName)
		}
	}
	printAndLogTime("juice complete!")
}

// localityPartition
func localityPartition(filePointers []File, fileMap *map[string][]string) {
	nodes := getWorkerIPs()
	maxNum := int(math.Ceil(float64(len(filePointers)) / float64(len(nodes))))
	for _, file := range filePointers {
		workerIP := findFileMasterLoc(file.FileHash)
		if len((*fileMap)[workerIP]) < maxNum {
			(*fileMap)[workerIP] = append((*fileMap)[workerIP], file.FileName)
		} else {
			tmpMin := maxNum
			minIp := ""
			for _, nodeIp := range nodes {
				if len((*fileMap)[nodeIp]) < tmpMin {
					tmpMin = len((*fileMap)[nodeIp])
					minIp = nodeIp
				}
			}
			(*fileMap)[minIp] = append((*fileMap)[minIp], file.FileName)
		}
	}
}

//function used to partition input data to each node
func partitionTask(userExe string, taskType string, filePointers []File, interPrefix string) {
	//workerIPs := getWorkerIPs()
	var fileMap = make(map[string][]string) //map: nodeIP -> []nodeIP

	switch taskType {
	case "maple":
		localityPartition(filePointers, &fileMap)
		fmt.Println(fileMap)
		//init and assign tasks
		for workerIP, fileNames := range fileMap {

			task := Task{
				TaskID:      strconv.Itoa(taskCount),
				FileNames:   fileNames,
				TaskType:    "maple",
				InterPrefix: interPrefix,
				TaskExe:     userExe,
			}
			taskCount++
			//send task and wait for result
			go sendTask(task, workerIP)
		}
		printAndLogTime("partition Tasks finished")
	case "juice":
		////get all files needed
		//for _, Pfile := range filePointers{
		//	get(Pfile.FileName, "./temp/"+Pfile.FileName)
		//}

		switch JUICE_PARTITION {
		case "hash":
			//hash partition tasks to nodes already having the files
			for _, file := range filePointers {
				//workerIP := hashPartitionTask(file.FileHash, getWorkerHashes())
				workerIP := findFileMasterLoc(file.FileHash)
				fileMap[workerIP] = append(fileMap[workerIP], file.FileName)
			}

			//init and assign tasks
			for workerIP, fileNames := range fileMap {

				task := Task{
					TaskID:      strconv.Itoa(taskCount),
					FileNames:   fileNames,
					TaskType:    "juice",
					InterPrefix: interPrefix,
					TaskExe:     userExe,
				}
				taskCount++
				//send task and wait for result
				go sendTask(task, workerIP)
			}

		case "range":
			//range partition tasks
			localityPartition(filePointers, &fileMap)
			fmt.Println(fileMap)

			//init and assign tasks
			for workerIP, fileNames := range fileMap {

				task := Task{
					TaskID:      strconv.Itoa(taskCount),
					FileNames:   fileNames,
					TaskType:    "juice",
					InterPrefix: interPrefix,
					TaskExe:     userExe,
				}
				taskCount ++
				//send task and wait for result
				go sendTask(task, workerIP)
			}


		}
		printAndLogTime("partition Tasks finished")
	}

}

//send and call task on a worker
func sendTask(task Task, workerIP string) {
	taskMonitor.addTask(task, workerIP)
	err := client.sendTask(task, workerIP)
	errHandler(err, false, "run task on node "+getAlias(workerIP)+"Failed!")
	if err == nil {
		//mark task as complete
		taskMonitor.TaskStatus[task.TaskID] = true

		nodeIP, hasTask := taskMonitor.Task2Worker[task.TaskID]
		if hasTask {
			taskMonitor.Worker2Task[nodeIP] = removeStringFromSlice(taskMonitor.Worker2Task[nodeIP], task.TaskID)
			delete(taskMonitor.Task2Worker, task.TaskID)
		}

		fmt.Println(taskMonitor.TaskStatus)
		fmt.Println(taskMonitor.Task2Worker)

		//check if all tasks are complete
		allTrue := true
		for _, complete := range taskMonitor.TaskStatus {
			if !complete {
				allTrue = false
			}
		}
		if allTrue {
			//if all completed, start merging
			taskMonitor.reset()
			completeSignal <- "ok"
		}
	}
}

/*
	logic of master node partitionMergeResult
*/
func partitionMergeResult(interPrefix string) {
	var mapData = make(map[string]*map[string][]string)
	var nodeFileName = make(map[string]string)
	// merge and partition
	for i := 0; i < taskCount; i++ {
		id := strconv.Itoa(i)
		for key, value := range *(taskMonitor.TaskID2KeyMap[id]) {
			nodeIp := findFileMasterLoc(hash([]byte(key)))
			// get file name
			if _, ok := nodeFileName[nodeIp]; !ok {
				if regexp.MustCompile(`[a-zA-Z0-9]+$`).MatchString(key) {
					nodeFileName[nodeIp] = regexp.MustCompile(`[a-zA-Z0-9]+$`).FindString(key)
				}
			}

			if _, ok := mapData[nodeIp]; !ok {
				var tmp = make(map[string][]string)
				mapData[nodeIp] = &tmp
			}
			singleNodeData := mapData[nodeIp]
			//if preValue, ok := (*singleNodeData)[key]; !ok {
				//(*singleNodeData)[key] = value
			//} else {
			(*singleNodeData)[key] = append((*singleNodeData)[key], value...)
			//}
		}
	}
	// write file
	for nodeIp, value := range mapData {
		writeKeyMapToFiles(value, interPrefix+"_"+nodeFileName[nodeIp])
		put("./temp/"+interPrefix+"_"+nodeFileName[nodeIp]+".txt", interPrefix+"_"+nodeFileName[nodeIp], interPrefix)
	}
}

/*
merge juice results
*/
func mergeJuice(fileName string) {
	var juiceResult = make(map[string][]string)

	//todo: convert to string
	for i := 0; i < taskCount; i++ {
		id := strconv.Itoa(i)
		for key, value := range *(taskMonitor.TaskID2KeyMap[id]) {
			juiceResult[key] = append(juiceResult[key], value...)
		}
	}

	// write file
	writeJuiceResult(&juiceResult, "result")
	put("./temp/"+"result"+".txt", fileName, "output")

}

/*
input a slice of filenames wrapped in a single task, and send result to master node/corresponding fileMasters
*/
func mapleTask(task Task) *map[string][]string {
	var keyMap = make(map[string][]string) //map: key -> slice of values, i.e. 'you': ['1','1','1','1'] in word count

	//todo: for each file, run mapleExe and get a slice of (key,value) pairs
	p, err := plugin.Open("./apps/" + task.TaskExe + ".so")

	errHandler(err, false, "go plugin open failed!")
	mapleExe, err := p.Lookup(task.TaskExe)
	errHandler(err, false, "plugin lookup failed!")

	for _, fileName := range task.FileNames {

		//get files to local
		get(fileName, "./temp/"+fileName)

		time.Sleep(time.Second * 5)

		f, err := os.Open("./temp/" + fileName)

		errHandler(err, false, "open maple data file failed!")
		scanner := bufio.NewScanner(f)
		mapList, err := mapleExe.(func(*bufio.Scanner) ([]map[string]string, error))(scanner)
		errHandler(err, false, "run maple plugin failed!")

		f.Close()

		//aggregate values by key
		for _, pairMap := range mapList {
			for key, value := range pairMap {
				_, hasKey := keyMap[key]
				if hasKey {
					keyMap[key] = append(keyMap[key], value)
				} else {
					keyMap[key] = []string{value}
				}
			}
		}
	}

	return &keyMap

	//Alternatively,
	//Wrap maple result in .tar and send it to master node, let master merge all results
	//1. write small files: temp/prefix_key

	//Alternatively,
	//write small files and put them in SDFS
	//writeKeyMapToFiles(keyMap, task)
	//err := filepath.Walk("./temp/", putTempFile)
	//fmt.Printf("filepath.Walk() returned %v\n", err)

}

/*
single task juice
*/
func juiceTask(task Task) *map[string][]string {
	var juiceResult = make(map[string][]string)

	p, err := plugin.Open("./apps/" + task.TaskExe + ".so")

	errHandler(err, false, "go plugin open failed!")
	juiceExe, err := p.Lookup(task.TaskExe)
	errHandler(err, false, "plugin lookup failed!")

	for _, fileName := range task.FileNames {

		//get files to local
		get(fileName, "./temp/"+fileName)

		time.Sleep(time.Second * 5)

		var keyMap map[string][]string

		readKeyMapFromFiles(&keyMap, fileName)

		juiceMap, err := juiceExe.(func(map[string][]string) (map[string][]string, error))(keyMap)
		errHandler(err, false, "run maple plugin failed!")


		//aggregate values by key
		for key, value := range juiceMap {
			juiceResult[key] = value
		}
	}

	return &juiceResult

}

//listen to failSignal channel and run task reschedule
func taskFailureHandler() {
	for {
		nodeID := <-failSignal
		//if node fails, delete this node and reschedule all tasks on this node
		taskIDs, ok := taskMonitor.Worker2Task[nodeID]
		if !ok {
			continue
		}

		time.Sleep(time.Second * 20)

		//reschedule tasks
		for _, taskID := range taskIDs {
			task := taskMonitor.TaskID2Task[taskID]
			allWorkerIPs := getWorkerIPs()
			workerIP := allWorkerIPs[len(allWorkerIPs)-2]

			fmt.Println("rescheduling task to " + getAlias(workerIP))

			go sendTask(*task, workerIP)
		}
		taskMonitor.deleteNode(nodeID)
	}
}

//---------------------------Utils--------------------------------------
//write a map of [key][]values to a file
func writeKeyMapToFiles(PKeyMap *map[string][]string, nameKey string) {
	// create new file
	f, err := os.OpenFile("./temp/"+nameKey+".txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	errHandler(err, false, "Error opening new file")

	err = json.NewEncoder(f).Encode(PKeyMap)
	errHandler(err, false, "json dump failed!")

	f.Close()
}

func readKeyMapFromFiles(PKeyMap *map[string][]string, fileName string) {

	f, err := os.Open("./temp/" + fileName)
	errHandler(err, false, "Error opening new file")

	err = json.NewDecoder(f).Decode(PKeyMap)
	errHandler(err, false, "json dump failed!")

	f.Close()
}

//write Juice Result
func writeJuiceResult(PKeyMap *map[string][]string, fileName string) {
	// create new file
	f, err := os.OpenFile("./temp/"+fileName+".txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	errHandler(err, false, "Error opening new file")

	for key, value := range *PKeyMap{
		f.WriteString(key+"\t"+strings.Join(value,",")+"\n")
	}

	f.Close()
}

//https://stackoverflow.com/questions/34070369/removing-a-string-from-a-slice-in-go
func removeStringFromSlice(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

//get a slice of all available workers' IP in SDFS
func getWorkerIPs() []string {
	var nodeIPs []string
	for _, nodeIP := range NodeHash2IP {
		//if nodeIP != MASTER_IP {
		if true {
			nodeIPs = append(nodeIPs, nodeIP)
		}
	}
	return nodeIPs
}

func GetFilesByDir2(allFiles []File, dir2 string) []File {
	var result []File

	for _, file := range allFiles {
		if file.Dir2 == dir2 {
			result = append(result, file)
		}
	}

	return result
}
