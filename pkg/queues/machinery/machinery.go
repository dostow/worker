package machinery

import (
	"context"
	"os"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/apex/log"
	"github.com/dostow/worker/pkg/queues/sqs"
	"google.golang.org/api/option"
)

func loadConfig() (*config.Config, error) {
	return config.NewFromEnvironment()
}

func Server() (*machinery.Server, error) {
	cnf, err := loadConfig()
	if err != nil {
		return nil, err
	}
	if len(cnf.DefaultQueue) == 0 {
		cnf.DefaultQueue = "machinery_tasks"
	}
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
func Worker(consumerTag string, workerTasks map[string]interface{}) (*machinery.Worker, error) {
	server, err := startServer(workerTasks)
	if err != nil {
		return nil, err
	}
	worker := server.NewWorker(consumerTag, 0)

	errorhandler := func(err error) {
		log.Errorf("I am an error handler: %v", err)
	}

	pretaskhandler := func(signature *tasks.Signature) {
		log.Debugf("I am a start of task handler for: %s %s", signature.Name, signature.UUID)
	}

	posttaskhandler := func(signature *tasks.Signature) {
		log.Debugf("I am an end of task handler for: %s %s", signature.Name, signature.UUID)
	}

	worker.SetPostTaskHandler(posttaskhandler)
	worker.SetErrorHandler(errorhandler)
	worker.SetPreTaskHandler(pretaskhandler)
	return worker, nil
}

func LambdaWorker(consumerTag string, workerTasks map[string]interface{}) (*machinery.Worker, error) {
	cnf, err := loadConfig()
	if err != nil {
		return nil, err
	}
	if len(cnf.DefaultQueue) == 0 {
		cnf.DefaultQueue = "machinery_tasks"
	}
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
	server, err := machinery.NewServer(cnf)
	if err != nil {
		return nil, err
	}
	server.RegisterTasks(workerTasks)
	if strings.Contains(cnf.Broker, "sqs") {
		server.SetBroker(sqs.New(cnf))
	}
	worker := server.NewWorker(consumerTag, 0)

	errorhandler := func(err error) {
		log.WithError(err).Error("task failed")
	}

	pretaskhandler := func(signature *tasks.Signature) {
		log.Debugf("START: %s %s", signature.Name, signature.UUID)
	}

	posttaskhandler := func(signature *tasks.Signature) {
		log.Debugf("END: %s %s", signature.Name, signature.UUID)
	}

	worker.SetPostTaskHandler(posttaskhandler)
	worker.SetErrorHandler(errorhandler)
	worker.SetPreTaskHandler(pretaskhandler)

	return worker, nil
}
