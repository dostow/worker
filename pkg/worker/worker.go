package worker

import (
	"context"
	"encoding/json"

	m "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/apex/log"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/dostow/worker/pkg/messages"
	"github.com/dostow/worker/pkg/queues/machinery"
	"github.com/tidwall/gjson"
)

type WorkerHandler interface {
	Invoke(ctx context.Context, payload []byte) ([]byte, error)
	Handle(config, param, data, traceID string) error
	Name() string
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

type LambdaHandler struct {
	worker  *m.Worker
	handler lambda.Handler
}

func (h *LambdaHandler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	var err error
	sqsMessage := messages.SQSMessage{}
	if err = json.Unmarshal(payload, &sqsMessage); err == nil {
		log.WithField("msg", string(payload)).Debugf("received SQS message")
		for _, record := range sqsMessage.Records {
			log := log.WithField("message_id", record.MessageId)
			body := *record.Body
			if gjson.Get(body, "Name").Exists() {
				sig := &tasks.Signature{}
				if err = json.Unmarshal([]byte(body), &sig); err == nil {
					err = h.worker.Process(sig)
					if err != nil {
						log.WithError(err).WithField("record", record).Error("machinery could not process task from record")
					}
					// state, err := h.worker.GetServer().GetBackend().GetState(sig.UUID)
					// if err == nil {
					// 	fmt.Println(state)
					// }
					// if broker, ok := h.worker.GetServer().GetBroker().(*sqs.Broker); ok {
					// 	broker.DeleteReceiptHandler(record.ReceiptHandle)
					// }
				} else {
					log.WithError(err).WithField("body", body).Errorf("could not parse task signature - %s", err.Error())
					return nil, err
				}
			} else {
				log.WithField("msg", sqsMessage).Error("task signature is invalid")
				// if broker, ok := h.worker.GetServer().GetBroker().(*sqs.Broker); ok {
				// 	broker.DeleteReceiptHandler(record.ReceiptHandle)
				// }
			}
		}
		return nil, nil
	}
	log.WithField("msg", string(payload)).Error("could not handle unknown message")
	return h.handler.Invoke(ctx, payload)
}

// Run run the worker
func (w *Worker) Run() error {
	log.WithField("worker", *w).Debug("Run worker")
	handlers := map[string]interface{}{w.handler.Name(): w.handler.Handle}
	worker, err := machinery.Worker(w.ID, handlers)
	if err != nil {
		return err
	}
	log.SetLevel(log.DebugLevel)
	if w.Command == "lambda" {
		lambdaWorker, err := machinery.LambdaWorker(w.ID, handlers)
		if err != nil {
			return err
		}
		lambda.Start(&LambdaHandler{lambdaWorker, w.handler})
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
	} else {
		return worker.Launch()
	}
	return err
}

// NewWorker new worker
func NewWorker(handler WorkerHandler) *Worker {
	return &Worker{handler: handler}
}
