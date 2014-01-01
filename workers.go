package goworker

var (
	workers map[string]workerFunc
)

func init() {
	workers = make(map[string]workerFunc)
}

// Registers a goworker worker function. Class refers to the
// Ruby name of the class which enqueues the job. Worker
// is a function which accepts a queue and an arbitrary
// array of interfaces as arguments.
func Register(class string, worker workerFunc) {
	workers[class] = worker
}

func Workers() []string {
	mk := make([]string, len(workers))
	for k, _ := range workers {
		mk = append(mk, k)
	}
	return mk
}
