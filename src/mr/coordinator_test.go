package mr

import (
	"testing"
	"time"
)

func Test_Coordinator(t *testing.T) {
	m := MakeCoordinator([]string{"./pg-sherlock_holmes.txt", "./pg-metamorphosis.txt"}, 2)
	if len(m.FilesStatus) != 2 {
		t.Fatalf("pending files must be 2")
	}
	workerID := uuid()
	req := &GetTaskRequest{WorkerID: workerID}
	resp := &GetTaskResponse{}
	if err := m.GetTask(req, resp); err != nil {
		t.Fatalf("get task failed: %v", err)
	}
	if resp.NReduce != 2 {
		t.Fatalf("nReduce must be 2")
	}
	if len(resp.FilePaths) != 1 {
		t.Fatalf("resp FilePaths must be 1")
	}
	if len(m.Set[workerID].AssignedFiles) != 1 {
		t.Fatal("worker must be assigned 1 file")
	}

	time.Sleep(12 * time.Second)
	// worker timed out

	if m.Done() {
		t.Fatalf("not done yet")
	}
	// new cycle
	if err := m.GetTask(req, resp); err != nil {
		t.Fatalf("get task failed: %v", err)
	}
	if len(m.Set[workerID].AssignedFiles) != 1 {
		t.Fatal("worker must be assigned 1 file")
	}
	if err := m.GetTask(&GetTaskRequest{
		Done:     []string{"./pg-sherlock_holmes.txt"},
		WorkerID: workerID,
	}, resp); err != nil {
		t.Fatalf("get task failed: %v", err)
	}
	if len(m.Set[workerID].AssignedFiles) != 1 {
		t.Fatal(m.Set[workerID].AssignedFiles)
		t.Fatal("worker must be assigned 1 file")
	}
	if err := m.GetTask(&GetTaskRequest{
		Done:     []string{"./pg-metamorphosis.txt"},
		WorkerID: workerID,
	}, resp); err != nil {
		t.Fatalf("get task failed: %v", err)
	}
	if !m.Done() {
		t.Fatal("should done")
	}
}
