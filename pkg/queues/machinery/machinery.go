package machinery

import (
	"context"
	"os"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"google.golang.org/api/option"
)

func loadConfig() (*config.Config, error) {
	return config.NewFromEnvironment()
}

func Server() (*machinery.Server, error) {
	c, _ := loadConfig()

	cnf := &config.Config{
		Broker:       c.Broker,
		DefaultQueue: "machinery_tasks",
	}
	if len(c.ResultBackend) > 0 {
		cnf.ResultBackend = c.ResultBackend
	}
	cnf.DefaultQueue = "machinery_tasks"
	cnf.AMQP = nil
	if strings.Contains(cnf.Broker, "gcp") {
		// setup pubsub
		sf, _ := os.LookupEnv("SERVICE_ACCOUNT_FILE")

		unsplit := strings.Split(cnf.Broker, "/")
		pubsubClient, err := pubsub.NewClient(
			context.Background(),
			unsplit[2],
			option.WithServiceAccountFile(sf),
		)
		if err != nil {
			panic(err)
		}
		cnf.GCPPubSub = &config.GCPPubSubConfig{
			Client: pubsubClient,
		}
	}
	return machinery.NewServer(cnf)
}
func startServer(tasks map[string]interface{}) (*machinery.Server, error) {
	server, err := Server()
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
