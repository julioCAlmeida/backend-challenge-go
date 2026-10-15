// Command sqs-provision creates (idempotently) the FIFO queues, their DLQs with
// redrive policies, and the access policies of the broker:
//
//	wager-transactions.fifo  -> wager-transactions-dlq.fifo (maxReceiveCount=SQS_MAX_RECEIVE_COUNT)
//	wallet-events.fifo       -> wallet-events-dlq.fifo
//
// Access policies (deploy/sqs/README in ARCHITECTURE.md):
//   - producers (provider integrations) may only SendMessage to wager-transactions.fifo;
//   - wallet-service may receive/delete/change visibility on wager-transactions.fifo,
//     send to its DLQ and send to wallet-events.fifo;
//   - downstream consumers may only receive/delete on wallet-events.fifo.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/sqsx"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "sqs-provision:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	client, err := sqsx.NewClient(ctx, sqsx.Config{
		Region: env("AWS_REGION", "us-east-1"), Endpoint: os.Getenv("SQS_ENDPOINT"),
		AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	})
	if err != nil {
		return err
	}
	maxReceive, err := strconv.Atoi(env("SQS_MAX_RECEIVE_COUNT", "5"))
	if err != nil {
		return err
	}
	visibility, err := time.ParseDuration(env("SQS_VISIBILITY_TIMEOUT", "30s"))
	if err != nil {
		return err
	}
	account := env("AWS_ACCOUNT_ID", "000000000000")
	principal := func(user string) string { return fmt.Sprintf("arn:aws:iam::%s:user/%s", account, user) }
	service, producer, downstream := principal(env("SQS_SERVICE_USER", "wallet-service")),
		principal(env("SQS_PRODUCER_USER", "wager-producer")), principal(env("SQS_EVENTS_CONSUMER_USER", "events-consumer"))

	wagerQueue, wagerDLQ := env("SQS_WAGER_QUEUE", "wager-transactions.fifo"), env("SQS_WAGER_DLQ", "wager-transactions-dlq.fifo")
	eventsQueue, eventsDLQ := env("SQS_EVENTS_QUEUE", "wallet-events.fifo"), env("SQS_EVENTS_DLQ", "wallet-events-dlq.fifo")

	if err := waitReady(ctx, client); err != nil {
		return err
	}
	queues := []struct {
		spec       sqsx.QueueSpec
		statements []statement
	}{
		{sqsx.QueueSpec{Name: wagerQueue, DLQName: wagerDLQ, MaxReceiveCount: maxReceive, VisibilityTimeout: visibility}, []statement{
			{"ProducersSend", producer, []string{"sqs:SendMessage"}},
			{"ServiceConsume", service, []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"}},
		}},
		{sqsx.QueueSpec{Name: wagerDLQ}, []statement{
			{"ServiceDeadLetter", service, []string{"sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"}},
		}},
		{sqsx.QueueSpec{Name: eventsQueue, DLQName: eventsDLQ, MaxReceiveCount: maxReceive, VisibilityTimeout: visibility}, []statement{
			{"ServicePublish", service, []string{"sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"}},
			{"DownstreamConsume", downstream, []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueUrl"}},
		}},
	}
	for _, q := range queues {
		url, err := sqsx.Ensure(ctx, client, q.spec)
		if err != nil {
			return err
		}
		arn, err := queueARN(ctx, client, url)
		if err != nil {
			return err
		}
		policy, err := buildPolicy(arn, q.statements)
		if err != nil {
			return err
		}
		if _, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
			QueueUrl: aws.String(url), Attributes: map[string]string{string(types.QueueAttributeNamePolicy): policy},
		}); err != nil {
			return fmt.Errorf("set policy on %s: %w", q.spec.Name, err)
		}
		fmt.Println("queue ready:", url)
	}
	return nil
}

func waitReady(ctx context.Context, c *sqs.Client) error {
	for {
		_, err := c.ListQueues(ctx, &sqs.ListQueuesInput{})
		if err == nil {
			return nil
		}
		fmt.Fprintln(os.Stderr, "waiting for SQS:", err)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Second):
		}
	}
}

func queueARN(ctx context.Context, c *sqs.Client, url string) (string, error) {
	out, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return "", err
	}
	return out.Attributes[string(types.QueueAttributeNameQueueArn)], nil
}

type statement struct {
	sid       string
	principal string
	actions   []string
}

func buildPolicy(arn string, sts []statement) (string, error) {
	type st struct {
		Sid       string            `json:"Sid"`
		Effect    string            `json:"Effect"`
		Principal map[string]string `json:"Principal"`
		Action    []string          `json:"Action"`
		Resource  string            `json:"Resource"`
	}
	doc := struct {
		Version   string `json:"Version"`
		Statement []st   `json:"Statement"`
	}{Version: "2012-10-17"}
	for _, s := range sts {
		doc.Statement = append(doc.Statement, st{Sid: s.sid, Effect: "Allow", Principal: map[string]string{"AWS": s.principal}, Action: s.actions, Resource: arn})
	}
	b, err := json.Marshal(doc)
	return string(b), err
}
