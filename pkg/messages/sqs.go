package messages

import awssqs "github.com/aws/aws-sdk-go/service/sqs"

// SQSMessage an sqs message
type SQSMessage struct {
	Records []*awssqs.Message `json:"Records"`
}
