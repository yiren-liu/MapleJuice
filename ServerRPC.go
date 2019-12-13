package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

//Server struct containing info and funcs of an RPC server
type Server struct {
	IP    string //Ip address of server
	Alias string //Alias of server
}
type GetFileReq struct {
	Node     Node //from node
	FileName string
}

type PutFileReq struct {
	NodeIP    string
	FileName  string
	Src       string
	TimeStamp time.Time
	Dir2 string
}

type DHTReq struct {
	//NodeHashKeys []int
	AllFile map[int]string
}

type ReceiveReq struct {
	FromNode  Node
	FileName  string
	FileSize  int64
	Dst       string
	Dir2	string// Secondary directory: "TaskID/"
	TimeStamp time.Time
}

type SendReq struct {
	FromNode Node
	FileName string
	Src      string
	Dir2 string
}

type SendRes struct {
	FileName  string
	TimeStamp time.Time
	FileSize int64
	Dir2 string
}

type PopWndReq struct {
	FileName string
}

//RPC struct stores info for RPC connection
type RPC struct {
	//fileMans map[string]*FileManager
	DownloadServer *net.Listener
}

type StartMapleReq struct {
	WorkType string
	MapleExe string
	MapleNum int
	InterPrefix string
	DataDir2 string
	DeleteInter string
}

func initRPC() *RPC {
	server, err := net.Listen("tcp", DownloadPort)
	errHandler(err, false, "Error listening to tcp conn")
	printAndLogTime("Start listening tcp")
	return &RPC{&server}
}

//init a new server from a Node and returns the pointer to the Server struct
func initServer(node Node) *Server {
	return &Server{node.IP, node.Alias}
}

func (srv *Server) listenAndServe(Rpc RPC) {
	if err := rpc.RegisterName("Rpc", &Rpc); err != nil {
		errHandler(err, false, "RPC failed to register.")
		return
	}

	listener, err := net.Listen("tcp", RpcPort)
	if err != nil {
		log.Fatal("Listen TCP error:", err)
	}

	//KEEP SERVING
	for {
		conn, err := listener.Accept()
		if err != nil {
			errHandler(err, false, "Error when listening to RPC connection")
		}
		go rpc.ServeConn(conn)
	}
}

//------------------------------RPC FOR SERVER--------------------------------
//Grep rpc API for local grep
func (p *RPC) Grep(req GrepRequest, res *string) error {
	*res = grepLog(req)
	return nil
}

func (p *RPC) DeleteFile(fileName string, res *bool) error {
	//p.mux.Lock()
	//defer p.mux.Unlock()
	*res = true
	DHTFileDeleteHandler(fileName)
	return nil
}

func (p *RPC) DeleteMyFile(fileName string, res *bool) error {
	mux.Lock()
	defer mux.Unlock()

	*res = true
	_ = deleteFile(fileName)
	removeFile(&MySDFSFiles, hash([]byte(fileName)))

	return nil
}

//------------------------------RPC FOR INIT--------------------------------
func (p *RPC) UpdateDHT(req DHTReq, res *bool) error {
	//NodeHashKeys = req.NodeHashKeys
	AllFiles = req.AllFile
	fmt.Println("update All file succeed!")
	fmt.Println(AllFiles)
	return nil
}

//put file into DHT
func (p *RPC) PutFile(req PutFileReq, res *bool) error {
	*res = true
	fmt.Println("receive PutFile Call")
	*res = DHTFilePutHandler(req.FileName, req.TimeStamp, req.NodeIP, req.Src, req.Dir2)
	return nil
}

func (p *RPC) PopWindow(req PopWndReq, res *bool) error {
	flag := false
	*res = false
	fmt.Printf("Input file %s conflicts! Last file have been uploaded within 1 min!\n", req.FileName)
	fmt.Println("Are you sure to put this file? (y/n)")
	go func() {
		for {
			result, err := reader.ReadString('\n')
			if flag {
				return
			}
			fmt.Println(result)
			in := strings.TrimSuffix(result, "\n")
			errHandler(err, false, "error when reading input")
			if in == "y" {
				input <- true
				break
			} else if in == "n" {
				input <- false
				break
			} else {
				fmt.Println("Invalid Command")
				fmt.Println("Are you sure to put this file? (y/n)")
			}
		}
	}()
	select {
	case i := <-input:
		*res = i
	case <-time.After(30 * time.Second):
		*res = false
		flag = true
	}
	return nil
}

//------------------------------RPC FOR FILE DOWNLOAD & UPLOAD--------------------------------

//RPC callee listens and accepts tcp dial, then writes connection into dst file
func (p *RPC) ReceiveFile(req ReceiveReq, res *bool) error {
	*res = false

	//mux.Lock()
	//defer mux.Unlock()
	// server start listenning
	listener := *p.DownloadServer

	go func() {
		// accept connection
		conn, err := listener.Accept()
		errHandler(err, false, "Error accepting tcp conn")

		// create new file
		f, err := os.OpenFile(req.Dst+req.FileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		errHandler(err, false, "Error opening new file")
		defer f.Close()

		// accept file from client & write to new file
		printAndLogTime("Receiving file from " + getAlias(req.FromNode.IP) + "....")
		_, err = io.Copy(f, conn)
		errHandler(err, false, "Error io copying from connection")
		conn.Close()

		//s, _ := f.Stat()
		//if s.Size() != req.FileSize {
		//	printAndLogTime("File Size different! " + strconv.Itoa(int(s.Size())) + " received but should be: " + strconv.Itoa(int(req.FileSize)))
		//	err = errors.New("FileSize not equal!")
		//}

		if err == nil {
			info, _ := f.Stat()
			printAndLogTime("Receive complete!" + strconv.Itoa(hash([]byte(req.FileName))) +" Size : " + strconv.Itoa(int(info.Size())) + "   ")
			//change local file list
			removeFile(&MySDFSFiles, hash([]byte(req.FileName)))
			MySDFSFiles = append(MySDFSFiles, File{
				FileHash:  hash([]byte(req.FileName)),
				FileName:  req.FileName,
				TimeStamp: req.TimeStamp,
				Dir2: req.Dir2,
			})
		}
	}()

	*res = true

	return nil
}

//RPC callee listens and accepts tcp dial, then writes file into the connection
func (p *RPC) SendFile(req SendReq, res *SendRes) error {

	//mux.Lock()
	//defer mux.Unlock()
	// server start listening
	listener := *p.DownloadServer

	go func() {
		// accept connection
		conn, err := listener.Accept()
		errHandler(err, false, "Error accepting tcp conn")

		// read from file
		f, err := os.Open(req.Src)
		errHandler(err, false, "Error opening existing file: "+req.Src)
		defer f.Close()
		info, _ := f.Stat()

		// write from file to connection
		printAndLogTime("Sending file to " + getAlias(req.FromNode.IP) + "....")
		_, err = io.Copy(conn, f)
		errHandler(err, false, "Error io copying to connection")
		conn.Close()
		if err == nil {
			printAndLogTime("Send file complete! Size: " + strconv.Itoa(int(info.Size())) + "   ")
			if req.Dir2 != ""{
				res.Dir2 = req.Dir2
			} else {
				*res = SendRes{
					FileName:  req.FileName,
					TimeStamp: time.Now(),
					Dir2:      MySDFSFiles.GetFileByName(req.FileName).Dir2,
					FileSize: info.Size(),
				}
			}
		}
	}()

	if req.Dir2 != ""{
		res.Dir2 = req.Dir2
	} else {
		*res = SendRes{
			FileName:  req.FileName,
			TimeStamp: time.Now(),
			Dir2:      MySDFSFiles.GetFileByName(req.FileName).Dir2,
		}
	}

	return nil
}

//report MySDFSfiles list
func (p *RPC) GetSDFSFileList(task Task, res *[]File) error {
	*res = MySDFSFiles
	return nil
}

//------------------------------RPC FOR MAPLEJUICE WORKER NODES--------------------------------

//master calls handleTask on worker nodes to perform tasks like map, reduce, merge ...
func (p *RPC) HandleTask(reqTask Task, res *TaskRes) error {
	res.TaskID = reqTask.TaskID

	switch reqTask.TaskType {
	case "maple":
		//run maple

		res.TaskKeyMap = *mapleTask(reqTask)
		res.taskStatus = "complete"

	case "juice":
		//run juice

		res.TaskKeyMap = *juiceTask(reqTask)
		res.taskStatus = "complete"

	}
	return nil
}
// Start Master Maple or juice logic
func (p *RPC) StartMaple(req StartMapleReq, res *bool) error {

	switch req.WorkType {
	case "Maple":
		//run maple
		Maple(req.MapleExe, req.MapleNum, req.InterPrefix, req.DataDir2)
	case "Juice":
		//run juice
		Juice(req.MapleExe, req.MapleNum, req.InterPrefix, req.DataDir2, req.InterPrefix)
	case "MapleJuice":
		//run merge
		mapleJuice(req.MapleExe, req.MapleNum, req.InterPrefix, req.DataDir2, req.InterPrefix)
	}
	*res = true
	return nil
}

//------------------------------RPC FOR MAPLEJUICE MASTER NODES--------------------------------





//master receives maple result: keyMap from nodes
//func (p *RPC) recvKeyMap(taskRes TaskRes, res *bool) error {
//	*res = true
//
//	//add keyMap to task monitor
//	taskMonitor.addKeyMap(taskRes.TaskID, &taskRes.TaskKeyMap)
//
//	//mark
//
//	return nil
//}


//------------------------------HELPER FUNCTIONS--------------------------------

func deleteFile(fileName string) error {
	cmd := exec.Command("rm", "./data/"+fileName)
	err := cmd.Run()
	return err
}
