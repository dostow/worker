package machinery

import (
	"os"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func loadConfig() (*config.Config, error) {
	return config.NewFromEnvironment()
}

func makeServer() (*machinery.Server, error) {
	uri := os.Getenv("redisURI")
	if uri == "" {
		uri = "redis://127.0.0.1:6379"
	}

	cnf := &config.Config{
		Broker:        uri,
		DefaultQueue:  "machinery_tasks",
		ResultBackend: uri,
	}
	return machinery.NewServer(cnf)
}
func startServer(tasks map[string]interface{}) (*machinery.Server, error) {
	server, err := makeServer()
	if err != nil {
		return nil, err
	}
	return server, server.RegisterTasks(tasks)
}

// Worker a machinery worker
func Worker(consumerTag string, workerTasks map[string]interface{}) error {
	server, err := startServer(workerTasks)
	if err != nil {
		return err
	}
	worker := server.NewWorker(consumerTag, 0)

	errorhandler := func(err error) {
		log.ERROR.Println("I am an error handler:", err)
	}

	pretaskhandler := func(signature *tasks.Signature) {
		log.INFO.Println("I am a start of task handler for:", signature.Name)
	}

	posttaskhandler := func(signature *tasks.Signature) {
		log.INFO.Println("I am an end of task handler for:", signature.Name)
	}

	worker.SetPostTaskHandler(posttaskhandler)
	worker.SetErrorHandler(errorhandler)
	worker.SetPreTaskHandler(pretaskhandler)
	return worker.Launch()
}
