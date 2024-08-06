package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func uuid() string {
	hostname, _ := os.Hostname()
	postfix, _ := exec.Command("uuidgen").Output()
	return fmt.Sprintf("%s-%s", hostname, postfix)
}

type (
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
)

// main/mrworker.go calls this function.
func Worker(mapf MapFunc, reducef ReduceFunc) {

	// Your worker implementation here.
	for {
		req := &GetTaskRequest{
			WorkerID: uuid(),
		}
		resp := &GetTaskResponse{}
		if CallGetTask(req, resp) == nil {
			// var wg sync.WaitGroup
			mu := sync.Mutex{}
			var waitReducer sync.WaitGroup
			for mapTaskID, path := range resp.FilePaths {
				c := sync.NewCond(&mu)
				fileTable := map[int]*os.File{}
				go func(path string) {
					log.Printf("worker %s is processing file %s", req.WorkerID, path)
					c.L.Lock()
					defer c.L.Unlock()
					content, readFileErr := os.ReadFile(path)
					if readFileErr != nil {
						log.Fatalf("cannot read %v", path)
					}
					keyval := mapf(path, string(content))
					sort.Sort(ByKey(keyval))
					sharded := map[int][]KeyValue{}
					for _, pair := range keyval {
						reduceTaskID := ihash(pair.Key) % resp.NReduce
						if _, exist := fileTable[reduceTaskID]; !exist {
							file, createFileErr := os.CreateTemp("", intermediateFile(mapTaskID, reduceTaskID))
							if createFileErr != nil {
								log.Fatalf("create intermediate file failed: %v", createFileErr)
							}
							fileTable[reduceTaskID] = file
						}
						sharded[reduceTaskID] = append(sharded[reduceTaskID], pair)
					}
					for reduceTaskID, intermediate := range sharded {
						file, exist := fileTable[reduceTaskID]
						if !exist {
							log.Fatalf("file %s not found", file.Name())
						}
						content, marshalErr := json.Marshal(intermediate)
						if marshalErr != nil {
							log.Fatalf("marshal map result failed: %v", marshalErr)
						}
						_, writeErr := file.Write(content)
						if writeErr != nil {
							log.Fatalf("write map result to intermediate file failed: %v", writeErr)
						}
						c.Signal()
					}
				}(path)
				for i := 0; i < resp.NReduce; i++ {
					go func() {
						log.Printf("reducer %d launched", i)
						waitReducer.Add(1)
						defer waitReducer.Done()
						c.L.Lock()
						defer c.L.Unlock()
						for len(fileTable) == 0 {
							log.Println("waiting for mapper to create file")
							c.Wait()
						}
						for reduceTaskID, file := range fileTable {
							content, readErr := os.ReadFile(file.Name())
							if readErr != nil {
								log.Fatalf("can not read intermediate file %s", file.Name())
							}
							var intermediate []KeyValue
							if marshalErr := json.Unmarshal(content, &intermediate); marshalErr != nil {
								log.Fatalf("can not marshal from byte to variable of type %T", intermediate)
							}
							file, createFileErr := os.CreateTemp("", finalFile(reduceTaskID))
							if createFileErr != nil {
								log.Fatalf("create temp final file failed: %v", createFileErr)
							}
							i := 0
							for i < len(intermediate) {
								j := i + 1
								for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
									j++
								}
								var values []string
								for k := i; k < j; k++ {
									values = append(values, intermediate[k].Value)
								}
								output := reducef(intermediate[i].Key, values)

								// this is the correct format for each line of Reduce output.
								fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

								i = j
							}
							log.Printf("done reducing file %s, removing from file table", file.Name())
							delete(fileTable, reduceTaskID)
							finalFilePath := finalFile(reduceTaskID)
							_ = os.Rename(file.Name(), finalFilePath)
							log.Printf("renamed %s to %s", file.Name(), finalFilePath)
							break
						}
					}()
				}
			}
			waitReducer.Wait()
		} else {
			break
		}
	}
}

func intermediateFile(mapTaskID, reduceTaskID int) string {
	currentDir, _ := os.Getwd()
	return fmt.Sprintf("%s/%d-%d", currentDir, mapTaskID, reduceTaskID)
}

func finalFile(reduceTaskID int) string {
	currentDir, _ := os.Getwd()
	return fmt.Sprintf("%s/mr-out-%d", currentDir, reduceTaskID)
}

func CallGetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	return call("Coordinator.GetTask", req, resp)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return fmt.Errorf("dialing: %w", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
