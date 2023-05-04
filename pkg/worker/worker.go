package worker

import (
	"context"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/apex/log"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/dostow/worker/pkg/queues/machinery"
)

type WorkerHandler interface {
	Invoke(ctx context.Context, payload []byte) ([]byte, error)
	Handle(config, param, data, traceID string) error
}

// Worker a fcm worker that sends messages to centrifuge
type Worker struct {
	Name    string `help:"name of service"`
	ID      string `help:"worker id"`
	Build   string `help:"build"`
	Command string `help:"command to run"`
	Config  string `help:"general configuration in json"`
	Param   string `help:"parameters for request in json"`
	Data    string `help:"data to send"`
	handler WorkerHandler
}

// Run run the worker
func (w *Worker) Run() error {
	log.SetLevel(log.DebugLevel)
	if w.Command == "lambda" {
		lambda.Start(w.handler)
	} else if w.Command == "send" {
		return w.handler.Handle(w.Config, w.Param, w.Data, "")
	} else if w.Command == "dispatch" {
		server, err := machinery.Server()
		if err != nil {
			return err
		}
		sig := &tasks.Signature{
			Name: w.Name,
			// RetryCount: 3,
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: w.Config,
				},
				{
					Type:  "string",
					Value: w.Param,
				},
				{
					Type:  "string",
					Value: w.Data,
				},
				{
					Type:  "string",
					Value: "s",
				},
			},
		}
		_, err = server.SendTask(sig)

		return err
	}
	handlers := map[string]interface{}{w.Name: w.handler.Handle}
	return machinery.Worker(w.ID, handlers)
}

// NewWorker new worker
func NewWorker(handler WorkerHandler) *Worker {
	return &Worker{handler: handler}
}
