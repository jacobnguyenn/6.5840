package mr

type Fs interface {
	ReadFile(path string) (content []byte, err error)
	WriteFile(name string, data []byte) error
	Getwd() (dir string, err error)
}

type localFs struct {
}

type remoteFs struct {
}
