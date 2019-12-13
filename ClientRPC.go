package main

import (
	"io"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type Client struct {
	IP         string
	Alias      string
	rpcClients map[string]*rpc.Client //stores RPC connection from nodeIP -> rpc.clients
}
//------------------------------HELP FUNCTIONS--------------------------------
func initClient(selfNode Node) *Client {
	return &Client{IP: selfNode.IP, Alias: selfNode.Alias, rpcClients: map[string]*rpc.Client{}}
}

//dials a node's rpc server if the connection is not established or closed, otherwise reuse conn from the map
func (c *Client) Connect(nodeIP string) (*rpc.Client, error) {
	_, exists := c.rpcClients[nodeIP]

	if exists == false {
		client, err := rpc.Dial("tcp", nodeIP +RpcPort)
		if err != nil {
			errHandler(err, false, "Error when dialing rpc to" + getAlias(nodeIP))
			return nil, err
		}
		printAndLogTime("RPC tcp connection established from" + selfNode.Alias + "to" + getAlias(nodeIP))
		c.rpcClients[nodeIP] = client
	}

	return c.rpcClients[nodeIP], nil
}

func (c *Client) Delete(nodeIP string) error {
	delete(c.rpcClients, nodeIP)
	return nil
}

//upload a file to another node
func (c *Client) uploadFile(fileName string, timeStamp time.Time, src string, serverIP string, dir2 string) error {
	var res bool

	// connect to server
	client, _ := c.Connect(serverIP)
	if err := client.Call("Rpc.ReceiveFile", ReceiveReq{
		FromNode: selfNode,
		FileName: fileName,
		FileSize: getFileSize(fileName),
		Dst:      "./data/",
		Dir2: dir2,
		TimeStamp: timeStamp,
	}, &res); err != nil {
		errHandler(err, false, "Error when calling Rpc.ReceiveFile on "+serverIP)
		return err
	}
	conn, err := net.Dial("tcp", serverIP +DownloadPort)
	errHandler(err, false, "Error when dialing tcp to "+ serverIP)
	defer conn.Close()

	// open file to upload
	//mux.Lock()
	//defer mux.Unlock()

	printAndLogTime("Uploading " + strconv.Itoa(hash([]byte(fileName)))+ " to "+ getAlias(serverIP) + "....")
	f, err := os.Open(src + fileName)
	errHandler(err, false, "Error when opening file" + src+fileName)
	defer f.Close()
	info, err := f.Stat()

	// upload
	_, err = io.Copy(conn, f)
	errHandler(err, false, "Error when upload io copying file :" + src+fileName)
	if err == nil{
		printAndLogTime("Upload complete! Size: " + strconv.Itoa(int(info.Size()))+ "   ")
	}

	return err
}

//download a file from another node
func (c *Client) downloadFile(fileName string, src string, dstFileName string, serverIP string, dir2 string) error {
	var res SendRes

	// connect to server
	client, _ := c.Connect(serverIP)
	if err := client.Call("Rpc.SendFile", SendReq{FromNode: selfNode, FileName:fileName, Src:src, Dir2: dir2}, &res); err != nil {
		errHandler(err, false, "Error when calling Rpc.SendFile on "+serverIP)
		return err
	}
	conn, err := net.Dial("tcp", serverIP +DownloadPort)
	errHandler(err, false, "Error when dialing tcp to "+ serverIP)
	defer conn.Close()


	dir2 = res.Dir2
	// open new file to write to
	//mux.Lock()
	//defer mux.Unlock()

	f, err := os.OpenFile(dstFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	errHandler(err, false, "Error when opening new file" + dstFileName)
	defer f.Close()

	// download
	printAndLogTime("Downloading file from "+ getAlias(serverIP) + "....")
	_, err = io.Copy(f, conn)
	errHandler(err, false, "Error when download io copying file :" + dstFileName)

	//s, _ := f.Stat()
	//if s.Size() != res.FileSize {
	//	printAndLogTime("File Size different! " + strconv.Itoa(int(s.Size())) + " received but should be: " + strconv.Itoa(int(res.FileSize)))
	//	err = errors.New("FileSize not equal!")
	//}

	if err == nil{
		info, _ := f.Stat()
		printAndLogTime("Download complete! Size : " + strconv.Itoa(int(info.Size()))+ "   ")


		if dstFileName[:7] == "./data/" {
			removeFile(&MySDFSFiles, hash([]byte(fileName)))
			MySDFSFiles = append(MySDFSFiles, File{
				FileHash:  hash([]byte(fileName)),
				FileName:  fileName,
				TimeStamp: res.TimeStamp,
				Dir2: dir2,
			})
		}
	}

	printAndLogTime("file: "+fileName+ " received!")

	return err
}

//------------------------------FOR MASTER FUNCTIONS--------------------------------
// send DHT data from master to new client
func (c *Client) updateDHT(node Node) error {
	var res bool
	client, _ := c.Connect(node.IP)
	if err := client.Call("Rpc.UpdateDHT", DHTReq{
		//NodeHashKeys: NodeHashKeys,
		AllFile:      AllFiles,
	}, &res); err != nil {
		return err
	}
	return nil
}
// file conflict, let user make choice
func (c *Client) callPopWindow(ip string, fileName string) (error, bool) {
	var res bool
	client, _ := c.Connect(ip)
	if err := client.Call("Rpc.PopWindow", PopWndReq{fileName}, &res); err != nil {
		return err, false
	}
	return nil, res
}
//------------------------------FOR CLIENT FUNCTIONS--------------------------------

func (c *Client) putFile(fileName string, timeStamp time.Time, fromNodeIP string, toNodeIP string, src string, dir2 string) (error, bool) {
	var res bool

	client, _ := c.Connect(toNodeIP)
	//fmt.Println(client)
	if err := client.Call("Rpc.PutFile", PutFileReq{
		NodeIP:    fromNodeIP,
		FileName:  fileName,
		TimeStamp: timeStamp,
		Src:       src,
		Dir2:dir2,
	}, &res); err != nil {
		return err, false
	}
	return nil, res
}

func (c *Client) removeFileAll(fileName string, nodeIP string) error {
	var res bool

	client, _ := c.Connect(nodeIP)
	if err := client.Call("Rpc.DeleteFile", fileName, &res); err != nil {
		return err
	}
	return nil
}


func (c *Client) removeFileAt(fileName string, nodeIP string) error {
	var res bool

	client, _ := c.Connect(nodeIP)
	if err := client.Call("Rpc.DeleteMyFile", fileName, &res); err != nil {
		return err
	}
	return nil

}

//------------------------------FOR MAPLEJUICE--------------------------------
func (c *Client) sendTask(task Task, nodeIP string) error {
	var res TaskRes

	client, _ := c.Connect(nodeIP)

	if err := client.Call("Rpc.HandleTask", task, &res); err != nil {
		return err
	}

	taskMonitor.addKeyMap(task.TaskID, &(res.TaskKeyMap))

	return nil

}
/*
 * rpc start maple
 * workType: Maple, Juice, MapleJuice
 */
func (c *Client) startMaple(workType string, mapleExe string, mapleNum int, interPrefix string, dataDir2 string, deleteInter string, nodeIP string) error {
	var res bool
	client, _ := c.Connect(nodeIP)

	if err := client.Call("Rpc.StartMaple", StartMapleReq{
		WorkType:    workType,
		MapleExe:    mapleExe,
		MapleNum:    mapleNum,
		InterPrefix: interPrefix,
		DataDir2:    dataDir2,
		DeleteInter: deleteInter,
	}, &res); err != nil {
		return err
	}

	return nil

}

//use RPC to get all nodes' SDFS file List
func (c *Client) getAllSDFSFiles() []File {
	var result []File

	for _, nodeIP := range NodeHash2IP {
		var temp []File

		client, _ := c.Connect(nodeIP)

		client.Call("Rpc.GetSDFSFileList", Task{}, &temp)

		result = append(result, temp...)

	}

	//drop duplicates
		seen := make(map[string]struct{}, len(result))
		j := 0
		for _, file := range result {
			if _, ok := seen[file.FileName]; ok {
			continue
		}
		seen[file.FileName] = struct{}{}
		result[j] = file
		j++
	}
		return result[:j]

}




//func (c *Client) sendKeyMap(keyMap map[string][]string, nodeIP string) error {
//	var res bool
//
//	client, _ := c.Connect(nodeIP)
//
//	if err := client.Call("Rpc.recvKeyMap", keyMap, &res); err != nil {
//		return err
//	}
//	return nil
//}

//------------------------------FOR HELPER FUNCTIONS--------------------------------
func getFileSize(fileName string) int64 {
	//mux.Lock()
	//defer mux.Unlock()

	f, err := os.Open("./data/" + fileName)
	defer f.Close()
	errHandler(err, false, "Error when opening file: " + fileName)
	if err == nil {
		s, _ := f.Stat()
		return s.Size()
	}

	return 0
}