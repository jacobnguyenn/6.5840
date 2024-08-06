package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	WorkerTimeOut = 10 * time.Second
)

type Status int

const (
	Pending Status = iota
	Processing
	Done
)

type Coordinator struct {
	// Your definitions here.
	NReduce int
	Set     Workers
	Lock    sync.Mutex
	// wait list
	FilesStatus map[string]Status
	Log         *log.Logger
}

type Workers map[string]*WorkerInfo

type WorkerInfo struct {
	AssignedFiles map[string]struct{}
	LastContact   time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	workerID := req.WorkerID
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id MUST be provided")
	}

	if _, registered := c.Set[workerID]; !registered {
		// new worker
		c.Log.Printf("new worker registering: %s", workerID)
		c.Set[workerID] = &WorkerInfo{
			LastContact:   time.Now(),
			AssignedFiles: make(map[string]struct{}),
		}
		c.Log.Printf("new worker registered: %s", workerID)
	} else {
		// existing worker
		c.Log.Printf("contact from existing worker: %s", workerID)
		c.Set[workerID].LastContact = time.Now()
		// remove done file from wait list
		for _, doneFile := range req.Done {
			for pendingFilePath := range c.FilesStatus {
				if pendingFilePath == doneFile {
					c.Log.Printf("worker %s finished processing file %s, mark it as done", workerID,
						doneFile)
					c.Lock.Lock()
					c.FilesStatus[doneFile] = Done
					delete(c.Set[workerID].AssignedFiles, doneFile)
					c.Lock.Unlock()
					c.Log.Printf("marked %s as done", doneFile)
				}
			}
		}
	}
	// if there is any file in wait list, give it to this worker
	for filePath, status := range c.FilesStatus {
		if status == Pending {
			resp.NReduce = c.NReduce
			c.Log.Printf("giving worker %s file %s", workerID, filePath)
			c.Lock.Lock()
			// one item a time
			resp.FilePaths = []string{filePath}
			c.Set[workerID].AssignedFiles[filePath] = struct{}{}
			c.FilesStatus[filePath] = Processing
			c.Lock.Unlock()
			break
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	for file, status := range c.FilesStatus {
		if status != Done {
			c.Log.Printf("file %s is not done", file)
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	pendingFiles := map[string]Status{}
	for _, file := range files {
		pendingFiles[file] = Pending
	}
	c := Coordinator{
		NReduce:     nReduce,
		FilesStatus: pendingFiles,
		Log:         log.New(os.Stdout, "", log.Ltime),
		Set:         make(Workers),
	}
	// Your code here.
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ticker.C:
				log.Printf("checking for dead worker")
				for id, workerInfo := range c.Set {
					if time.Now().Sub(workerInfo.LastContact) > WorkerTimeOut {
						c.Lock.Lock()
						for filePath := range workerInfo.AssignedFiles {
							c.Log.Printf("worker %s timed out, moving file %s back to wait list", id, filePath)
							c.FilesStatus[filePath] = Pending
						}
						delete(c.Set, id)
						c.Lock.Unlock()
					}
				}
			}
		}
	}()
	c.server()
	return &c
}
