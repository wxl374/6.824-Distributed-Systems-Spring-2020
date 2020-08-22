package mr

import (
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Master struct {
	// Your definitions here.
	Phase    int
	Mapnr    int
	Reducenr int
	Undone   map[int]Task
	Doing    map[int]Task
	Mutex    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) AssignTask(args *Args, reply *Reply) error {
	reply.Valid = false
	reply.Done  = false

	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if m.Phase == Reduce && len(m.Undone) == 0 && len(m.Doing) == 0 {
		reply.Done = true
//		log.Printf("all task finished")
		return nil
	}

	if len(m.Undone) == 0 {
//		log.Printf("no task to assign undone:%d", len(m.Undone))
		return nil
	}

	reply.Valid    = true
	reply.Mapnr    = m.Mapnr
	reply.Reducenr = m.Reducenr

	var idx  int
	var task Task
	for idx, task = range m.Undone {
		task.Starttime = time.Now().Unix()
		reply.Task = task
		break
	}

	delete(m.Undone, idx)
	m.Doing[idx] = task

//	log.Printf("AssignTask undone %d doing %d type %d idx %d inputfile %s",
//		len(m.Undone), len(m.Doing), task.Type, task.Idx, task.Inputfile)

	return nil
}

func (m *Master) GetResult(args *Args, reply *Reply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	now := time.Now().Unix()
	if now - args.Task.Starttime >= 10 {
//		log.Printf("start %d now %d", args.Task.Starttime, now)
		return nil
	}

	delete(m.Doing, args.Task.Idx)
	if args.Task.Err != nil {
		args.Task.Err = nil
		m.Undone[args.Task.Idx] = args.Task
	} else {
		if m.Phase == Map && len(m.Undone) == 0 && len(m.Doing) == 0 {
//			log.Printf("all map task finished phase:%d mapnr:%d reducenr:%d undone:%d done:%d",
//				m.Phase, m.Mapnr, m.Reducenr, len(m.Undone), len(m.Doing))
			m.initReduceTask()
		}
	}

	return nil
}

func (m *Master) CheckTimeout() {
	for {
		m.Mutex.Lock()
		if m.Phase == Reduce && len(m.Undone) == 0 && len(m.Doing) == 0 {
			m.Mutex.Unlock()
			break
		}

		for _, task := range m.Doing {
			if time.Now().Unix() - task.Starttime >= 10 {
				delete(m.Doing, task.Idx)
				m.Undone[task.Idx] = task
			}
		}

		m.Mutex.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond) //100ms
	}

//	log.Printf("go routine CheckTimeout exit")
}

func (m *Master) initMapTask(files []string, nReduce int) {
	m.Phase    = Map
	m.Mapnr    = len(files)
	m.Reducenr = nReduce
	m.Undone   = make(map[int]Task)

	for idx, file := range files {
		task := Task{Type: Map, Idx: idx, Inputfile: file}
		m.Undone[idx] = task
	}

	m.Doing = make(map[int]Task)
}

//no lock, the caller checks when to init
func (m *Master) initReduceTask() {
	m.Phase = Reduce
	for i := 0; i < m.Reducenr; i++ {
		task := Task{Type: Reduce, Idx: i}
		m.Undone[i] = task
//		log.Printf("initReduceTask type:%d idx:%d", task.Type, task.Idx)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if m.Phase == Reduce && len(m.Undone) == 0 && len(m.Doing) == 0 {
		ret = true
//		log.Printf("all tasks finished")
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	log.SetFlags(log.Ltime | log.Lshortfile)

	m := Master{}
	m.initMapTask(files, nReduce)
	m.server()
	go m.CheckTimeout()

	return &m
}
