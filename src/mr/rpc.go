package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
const (
	Map    int = 0
	Reduce int = 1
)

type Task struct {
	Type      int
	Idx       int
	Inputfile string
	Starttime int64
	Err       error
}

//RetResult use it
type Args struct {
	Task Task
}

//AskForTask return it
type Reply struct {
	Valid    bool //no task to assign if equal to false
	Done     bool //all task finished if equal to true
	Mapnr    int
	Reducenr int
	Task     Task
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
