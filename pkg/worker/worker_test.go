package worker

import (
	"context"
	"reflect"
	"testing"

	m "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/backends/null"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/dostow/worker/pkg/queues/machinery"
)

func TestLambdaHandler_Invoke(t *testing.T) {
	handlers := map[string]interface{}{"hook": func(config, param, data, trace string) (string, error) {
		return "success", nil
	}}
	worker, _ := machinery.Worker("hook", handlers)
	worker.GetServer().SetBackend(null.New())
	// worker.GetServer().SetBroker(redis)
	type fields struct {
		worker  *m.Worker
		handler lambda.Handler
	}
	type args struct {
		ctx     context.Context
		payload []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"test",
			fields{
				worker,
				nil,
			},
			args{
				context.Background(),
				[]byte(`{
					"Records": [
						{
							"messageId": "5900aa90-e742-4007-b2a1-f13cf1ff3c49",
							"receiptHandle": "AQEBWv2HUdXv8bFhswG4vtB+mcp+mLS4HBZfOQsagm4mcyqi1i3cvyYU/Q7nbhjeAy0ySclD/1TcKDbrxAv8tb0fHw6RihRT0Yo7t5rE/zrXuiCBMcRN3+hW4L/WVlZXfVvM5or9T0i2l2VmUjoX6eD29FeAzIFbARAvSf581+i+ZfN2bGxSPMILhlcrBqlDYjEDysb7xb4H0FgCcJPTNkKS7hqrbOS+hdD6H8vICeOQOtjHTNRReqN0RnS4LIl8/WsHZd1gc+LBaMZfiv7wAYoSzRAdS3Dje/DPrgQHioMl14NNwY6H071NKZxqGzLC3TB2yOJcVIEMdfTEl/vh/NnDYh4A0I/Mwnj2HsGfC8st3EYjiLUv68CIkkO6aHf25KgMS1CqSkt6SofLGveNwXeFDg==",
							"body": "{\"UUID\":\"task_c449c7ef-031c-46bc-8405-ae202d882d2b\",\"Name\":\"hook\",\"RoutingKey\":\"\",\"ETA\":null,\"GroupUUID\":\"\",\"GroupTaskCount\":0,\"Args\":[{\"Name\":\"config\",\"Type\":\"string\",\"Value\":\"{\\\"headers\\\":{\\\"Content-Type\\\":\\\"application/json\\\"},\\\"host\\\":\\\"http://whatsapp:3000\\\"}\"},{\"Name\":\"params\",\"Type\":\"string\",\"Value\":\"{\\\"body\\\":{\\\"message\\\":\\\"Use this code to verify your phone: 832252\\\\nYou can also tap this link to verify your phone: https://vaza.cc/tpppgDyU67yCpDeo8\\\",\\\"phone\\\":\\\"2347026713923\\\",\\\"sessionId\\\":\\\"5ef0413b-e5a3-448e-9345-680f2f98922c\\\",\\\"type\\\":\\\"user\\\"},\\\"callback\\\":{\\\"method\\\":\\\"PUT\\\",\\\"url\\\":\\\"data.callback_url\\\"},\\\"method\\\":\\\"POST\\\",\\\"path\\\":\\\"send/message\\\"}\"},{\"Name\":\"data\",\"Type\":\"string\",\"Value\":\"{\\\"Data\\\":{\\\"action\\\":\\\"create_user_for_member\\\",\\\"appconfig\\\":{\\\"created_at\\\":\\\"2022-07-13T12:19:14.806742228Z\\\",\\\"custom\\\":{\\\"maxDispersalAmount\\\":10000,\\\"maxTransactionsPerDay\\\":100,\\\"minDispersalAmount\\\":1000},\\\"enabled\\\":true,\\\"id\\\":\\\"82fe9379b3461a337cbfafd7d2d4237504c0c635\\\",\\\"modified_at\\\":\\\"2022-09-11T10:08:30.583061261Z\\\",\\\"name\\\":\\\"mobile\\\",\\\"payment\\\":{\\\"enabled\\\":false,\\\"paystack\\\":{\\\"secret\\\":\\\"sk_test_076a247addd97dbf4112e4da8982973d50bac6b4\\\"}},\\\"transfers\\\":{\\\"enabled\\\":false}},\\\"appurl\\\":\\\"https://verify.vazapay.com/?action=verify\\\\u0026h=698acc2ecea095bd6f684ae0cdcbedf5f08e6f6d69f92104aa3b41dde6e92322\\\",\\\"code\\\":\\\"832252\\\",\\\"created_at\\\":\\\"2023-05-07T05:08:41.063386978Z\\\",\\\"id\\\":\\\"dadfedd2b58e8fc2ad34192a3f0ababd0d9d6c9d\\\",\\\"method\\\":\\\"phone\\\",\\\"modified_at\\\":\\\"2023-05-07T05:08:41.06383529Z\\\",\\\"origin\\\":\\\"api\\\",\\\"otphash\\\":\\\"698acc2ecea095bd6f684ae0cdcbedf5f08e6f6d69f92104aa3b41dde6e92322\\\",\\\"pull\\\":{\\\"previewLink\\\":\\\"https://vaza.cc/tpppgDyU67yCpDeo8?d=1\\\",\\\"shortLink\\\":\\\"https://vaza.cc/tpppgDyU67yCpDeo8\\\"},\\\"secret\\\":\\\"26e8a43a-1578-4967-9f8f-3ec8576a1bdc\\\",\\\"sms\\\":\\\"Use this code to verify your phone: 832252\\\\nYou can also tap this link to verify your phone\\\",\\\"status\\\":\\\"initiated\\\",\\\"user_id\\\":\\\"8abf07152c88f44ff24a6ef1a3344c3784cf9d17\\\",\\\"value\\\":{\\\"phone\\\":\\\"2347026713923\\\"}},\\\"GroupID\\\":\\\"5c84b77d17c3fa0001000025_mainapp\\\",\\\"GroupName\\\":\\\"mainapp\\\",\\\"Method\\\":\\\"POST\\\",\\\"Owner\\\":\\\"5c84b77d17c3fa0001000025\\\",\\\"StoreId\\\":\\\"5c84b77d17c3fa0001000025_mainapp_otpaction\\\",\\\"StoreName\\\":\\\"otpaction\\\",\\\"StoreTitle\\\":\\\"Otpaction\\\"}\"},{\"Name\":\"traceID\",\"Type\":\"string\",\"Value\":\"64b0d081-6d1c-456d-930e-063e405de1bc\"}],\"Headers\":{},\"Priority\":0,\"Immutable\":false,\"RetryCount\":0,\"RetryTimeout\":0,\"OnSuccess\":null,\"OnError\":null,\"ChordCallback\":null,\"BrokerMessageGroupId\":\"\",\"SQSReceiptHandle\":\"\",\"StopTaskDeletionOnError\":false,\"IgnoreWhenTaskNotRegistered\":false}",
							"attributes": {
								"ApproximateReceiveCount": "4",
								"SentTimestamp": "1683437779024",
								"SenderId": "098957433357",
								"ApproximateFirstReceiveTimestamp": "1683437779024"
							},
							"messageAttributes": {},
							"md5OfBody": "ec061791ba7d474f7696fc54f0eec962",
							"eventSource": "aws:sqs",
							"eventSourceARN": "arn:aws:sqs:af-south-1:098957433357:machinery_tasks",
							"awsRegion": "af-south-1"
						}
					]
				}`),
			},
			nil,
			false,
		},
		{
			"multiple records",
			fields{
				worker,
				nil,
			},
			args{
				context.Background(),
				[]byte(`{
					"Records": [
						{
							"messageId": "4ede67a5-b43d-45b8-b66c-ee0492f2455f",
							"receiptHandle": "AQEB+kBQ6XmEXpNbeoWgACUoQPPJt6jqDvRjz8JLjTPVFIy91CmyGYH1ckLNtQLOiBoUX40Hcas/w3P4K4dpjolLs9uwXLhQtg/e0/wZuErsPV8pWEgPVKrBrQbZTIxtrH0PrUJgBsDB5OMxiexdzdqlr+Vm10UNYp3gysbRPBzgpIsxx68pi9/s1cfbpuVkAx0f+0kneFi7I/zegt0iWGZHYCmKZyXXTF/+AI3+5MD2xU40ElDaRYbnI9MIyvfVj4hYlBuJdUB4YOkGTJdkLQzJeQmtK0dX8m6V7jjdEwbREHArK8VPftbTh2eDrhfctFCuJLKYLp+uDlY4kovSq8DYm98PS8IiXOKZJkyeiI1de9KzhvuTop3UiEy/RJVhu8tm14x6LuGppCHlwhP8vraZKQ==",
							"body": "{\"UUID\":\"task_c449c7ef-031c-46bc-8405-ae202d882d2b\",\"Name\":\"hook\",\"RoutingKey\":\"\",\"ETA\":null,\"GroupUUID\":\"\",\"GroupTaskCount\":0,\"Args\":[{\"Name\":\"config\",\"Type\":\"string\",\"Value\":\"{\\\"headers\\\":{\\\"Content-Type\\\":\\\"application/json\\\"},\\\"host\\\":\\\"http://whatsapp:3000\\\"}\"},{\"Name\":\"params\",\"Type\":\"string\",\"Value\":\"{\\\"body\\\":{\\\"message\\\":\\\"Use this code to verify your phone: 832252\\\\nYou can also tap this link to verify your phone: https://vaza.cc/tpppgDyU67yCpDeo8\\\",\\\"phone\\\":\\\"2347026713923\\\",\\\"sessionId\\\":\\\"5ef0413b-e5a3-448e-9345-680f2f98922c\\\",\\\"type\\\":\\\"user\\\"},\\\"callback\\\":{\\\"method\\\":\\\"PUT\\\",\\\"url\\\":\\\"data.callback_url\\\"},\\\"method\\\":\\\"POST\\\",\\\"path\\\":\\\"send/message\\\"}\"},{\"Name\":\"data\",\"Type\":\"string\",\"Value\":\"{\\\"Data\\\":{\\\"action\\\":\\\"create_user_for_member\\\",\\\"appconfig\\\":{\\\"created_at\\\":\\\"2022-07-13T12:19:14.806742228Z\\\",\\\"custom\\\":{\\\"maxDispersalAmount\\\":10000,\\\"maxTransactionsPerDay\\\":100,\\\"minDispersalAmount\\\":1000},\\\"enabled\\\":true,\\\"id\\\":\\\"82fe9379b3461a337cbfafd7d2d4237504c0c635\\\",\\\"modified_at\\\":\\\"2022-09-11T10:08:30.583061261Z\\\",\\\"name\\\":\\\"mobile\\\",\\\"payment\\\":{\\\"enabled\\\":false,\\\"paystack\\\":{\\\"secret\\\":\\\"sk_test_076a247addd97dbf4112e4da8982973d50bac6b4\\\"}},\\\"transfers\\\":{\\\"enabled\\\":false}},\\\"appurl\\\":\\\"https://verify.vazapay.com/?action=verify\\\\u0026h=698acc2ecea095bd6f684ae0cdcbedf5f08e6f6d69f92104aa3b41dde6e92322\\\",\\\"code\\\":\\\"832252\\\",\\\"created_at\\\":\\\"2023-05-07T05:08:41.063386978Z\\\",\\\"id\\\":\\\"dadfedd2b58e8fc2ad34192a3f0ababd0d9d6c9d\\\",\\\"method\\\":\\\"phone\\\",\\\"modified_at\\\":\\\"2023-05-07T05:08:41.06383529Z\\\",\\\"origin\\\":\\\"api\\\",\\\"otphash\\\":\\\"698acc2ecea095bd6f684ae0cdcbedf5f08e6f6d69f92104aa3b41dde6e92322\\\",\\\"pull\\\":{\\\"previewLink\\\":\\\"https://vaza.cc/tpppgDyU67yCpDeo8?d=1\\\",\\\"shortLink\\\":\\\"https://vaza.cc/tpppgDyU67yCpDeo8\\\"},\\\"secret\\\":\\\"26e8a43a-1578-4967-9f8f-3ec8576a1bdc\\\",\\\"sms\\\":\\\"Use this code to verify your phone: 832252\\\\nYou can also tap this link to verify your phone\\\",\\\"status\\\":\\\"initiated\\\",\\\"user_id\\\":\\\"8abf07152c88f44ff24a6ef1a3344c3784cf9d17\\\",\\\"value\\\":{\\\"phone\\\":\\\"2347026713923\\\"}},\\\"GroupID\\\":\\\"5c84b77d17c3fa0001000025_mainapp\\\",\\\"GroupName\\\":\\\"mainapp\\\",\\\"Method\\\":\\\"POST\\\",\\\"Owner\\\":\\\"5c84b77d17c3fa0001000025\\\",\\\"StoreId\\\":\\\"5c84b77d17c3fa0001000025_mainapp_otpaction\\\",\\\"StoreName\\\":\\\"otpaction\\\",\\\"StoreTitle\\\":\\\"Otpaction\\\"}\"},{\"Name\":\"traceID\",\"Type\":\"string\",\"Value\":\"64b0d081-6d1c-456d-930e-063e405de1bc\"}],\"Headers\":{},\"Priority\":0,\"Immutable\":false,\"RetryCount\":0,\"RetryTimeout\":0,\"OnSuccess\":null,\"OnError\":null,\"ChordCallback\":null,\"BrokerMessageGroupId\":\"\",\"SQSReceiptHandle\":\"\",\"StopTaskDeletionOnError\":false,\"IgnoreWhenTaskNotRegistered\":false}",
							"attributes": {
								"ApproximateReceiveCount": "5",
								"SentTimestamp": "1683440250276",
								"SenderId": "098957433357",
								"ApproximateFirstReceiveTimestamp": "1683440250276"
							},
							"messageAttributes": {},
							"md5OfBody": "ec061791ba7d474f7696fc54f0eec962",
							"eventSource": "aws:sqs",
							"eventSourceARN": "arn:aws:sqs:af-south-1:098957433357:machinery_tasks",
							"awsRegion": "af-south-1"
						},
						{
							"messageId": "2a5e3cb4-3f5b-4546-aabd-1afef4498b3b",
							"receiptHandle": "AQEB7Fpngx21HmRo1JqhbHrdJMs20UFugvSoWZ+9A/GVlVjTatNSe2hopAOj6jejR3Lh40K9DfwbsbkHbql4zIyzThvJOZVgUo/1oapocCwhI/k1RjTF8l7sr7FjSGLF6wiEAKszH8Wl4wBVcnPxTDLDGFqqH++6Ypys1/VLWOiIAAazKxfLyEM+WxFibJxB/3wl8jX8q8/2uQzgKJuitRBUx8uFdR60TMzZZae9j/9uXmR+/p15V0YhD5d8TIRp6oo8n+QrL3/FJ7bOAp8/AR8wdio02qpyvC8s+0ZJXPNE7+K8pLs1BxpabvEGMli6HyQkYVqRfGtRb5wCMvBQq6b162QigpjUwcj/e4z7ibcOtWuSQ9ctbeVEitylvadVvhI6jS7xCB1yQ5mzkmQVSBua5w==",
							"body": "{\n  \"config\": {\n      \"host\": \"https://enj5usapsxgq.x.pipedream.net\"\n  },\n  \"params\": {\n      \"method\": \"POST\",\n      \"body\": {\n          \"name\": \"Data.name\"\n      }\n  },\n  \"data\": \"{\\\"Data\\\":{\\\"name\\\":\\\"from-sqs\\\"}}\",\n  \"traceID\": \"Test\"\n}",
							"attributes": {
								"ApproximateReceiveCount": "5",
								"SentTimestamp": "1683440250328",
								"SenderId": "098957433357",
								"ApproximateFirstReceiveTimestamp": "1683440250328"
							},
							"messageAttributes": {},
							"md5OfBody": "dc4c3b5d480580bc3f1fcb645cf86209",
							"eventSource": "aws:sqs",
							"eventSourceARN": "arn:aws:sqs:af-south-1:098957433357:machinery_tasks",
							"awsRegion": "af-south-1"
						}
					]
				}`),
			},
			nil,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &LambdaHandler{
				worker:  tt.fields.worker,
				handler: tt.fields.handler,
			}
			got, err := h.Invoke(tt.args.ctx, tt.args.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("LambdaHandler.Invoke() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LambdaHandler.Invoke() = %v, want %v", got, tt.want)
			}
		})
	}
}
