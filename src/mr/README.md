# Distributed MapReduce
- [Worker](..%2Fmain%2Fmrworker.go)
- [Coordinator](..%2Fmain%2Fmrcoordinator.go)
Worker                           Coordinator
Worker      ---getTask()---->
            <---task---------
            

- Worker run concurrently.
- Worker poll for task from coordinator, read task input from file(s), execute task and send back the result to coordinator. 
- Coordinator should detect worker failure after 10 seconds timeout.
- The worker implementation should put the output of the X'th reduce task in the file `mr-out-X`.
- A `mr-out-X` file should contain one line per Reduce function output. The line should be generated with the Go "%v %v" format, called with the key and value. 
- The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
- Coordinator need to implement Done(): bool method to indicate that the MapReduce job is completely finished.
- When the job is completely finished, the worker processes should exit.
-  A simple way to implement this is to use the return value from call(): if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, so the worker can terminate too
