// Package sqsx wraps the AWS SQS client: configuration, queue provisioning,
// publishing and error classification.
package sqsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
)

// Config of the SQS client.
type Config struct {
	Region          string
	Endpoint        string // LocalStack / MiniStack endpoint; empty for AWS
	AccessKeyID     string
	SecretAccessKey string
}

// NewClient builds an SQS client with static credentials (the broker grants
// permissions per credential, see deploy/sqs/policies).
func NewClient(ctx context.Context, cfg Config) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.Region)}
	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	}), nil
}

// QueueSpec describes a FIFO queue with its dead-letter queue.
type QueueSpec struct {
	Name              string
	DLQName           string // empty: no DLQ
	MaxReceiveCount   int
	VisibilityTimeout time.Duration
}

// Ensure creates the queue (and its DLQ with the redrive policy) when missing.
// It is idempotent and returns the queue URL.
func Ensure(ctx context.Context, c *sqs.Client, spec QueueSpec) (string, error) {
	attrs := map[string]string{
		string(types.QueueAttributeNameFifoQueue):                 "true",
		string(types.QueueAttributeNameContentBasedDeduplication): "false",
		string(types.QueueAttributeNameMessageRetentionPeriod):    strconv.Itoa(4 * 24 * 3600),
	}
	if spec.VisibilityTimeout > 0 {
		attrs[string(types.QueueAttributeNameVisibilityTimeout)] = strconv.Itoa(int(spec.VisibilityTimeout.Seconds()))
	}
	if spec.DLQName != "" {
		dlqURL, err := Ensure(ctx, c, QueueSpec{Name: spec.DLQName})
		if err != nil {
			return "", err
		}
		out, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(dlqURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		})
		if err != nil {
			return "", err
		}
		redrive, _ := json.Marshal(map[string]string{
			"deadLetterTargetArn": out.Attributes[string(types.QueueAttributeNameQueueArn)],
			"maxReceiveCount":     strconv.Itoa(spec.MaxReceiveCount),
		})
		attrs[string(types.QueueAttributeNameRedrivePolicy)] = string(redrive)
	}
	created, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(spec.Name), Attributes: attrs})
	if err != nil {
		var exists *types.QueueNameExists
		if !errors.As(err, &exists) {
			return "", fmt.Errorf("create queue %s: %w", spec.Name, err)
		}
		url, err := QueueURL(ctx, c, spec.Name)
		if err != nil {
			return "", err
		}
		_, err = c.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(url), Attributes: withoutFifo(attrs)})
		return url, err
	}
	return aws.ToString(created.QueueUrl), nil
}

func withoutFifo(attrs map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range attrs {
		if k != string(types.QueueAttributeNameFifoQueue) {
			out[k] = v
		}
	}
	return out
}

// QueueURL resolves a queue name.
func QueueURL(ctx context.Context, c *sqs.Client, name string) (string, error) {
	out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		return "", Classify(fmt.Errorf("resolve queue %s: %w", name, err))
	}
	return aws.ToString(out.QueueUrl), nil
}

// Ping checks that the queue is reachable with the configured credentials.
func Ping(ctx context.Context, c *sqs.Client, queueURL string) error {
	_, err := c.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return Classify(err)
}

// Publisher sends messages to a FIFO queue.
type Publisher struct {
	client   *sqs.Client
	queueURL string
}

func NewPublisher(c *sqs.Client, queueURL string) *Publisher {
	return &Publisher{client: c, queueURL: queueURL}
}

// Publish sends body with the given group and deduplication ids.
func (p *Publisher) Publish(ctx context.Context, groupID, dedupID, body string, attrs map[string]string) error {
	in := &sqs.SendMessageInput{
		QueueUrl: aws.String(p.queueURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(groupID), MessageDeduplicationId: aws.String(dedupID),
	}
	if len(attrs) > 0 {
		in.MessageAttributes = map[string]types.MessageAttributeValue{}
		for k, v := range attrs {
			in.MessageAttributes[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
		}
	}
	_, err := p.client.SendMessage(ctx, in)
	return Classify(err)
}

// BatchEntry is one message of a batch send.
type BatchEntry struct {
	ID      string // unique within the batch
	GroupID string
	DedupID string
	Body    string
	Attrs   map[string]string
}

// PublishBatch sends up to 10 messages in one call. It returns the entries
// that failed (by ID); a non-nil error means the whole call failed.
func (p *Publisher) PublishBatch(ctx context.Context, entries []BatchEntry) (map[string]error, error) {
	in := &sqs.SendMessageBatchInput{QueueUrl: aws.String(p.queueURL)}
	for _, e := range entries {
		be := types.SendMessageBatchRequestEntry{
			Id: aws.String(e.ID), MessageBody: aws.String(e.Body),
			MessageGroupId: aws.String(e.GroupID), MessageDeduplicationId: aws.String(e.DedupID),
		}
		if len(e.Attrs) > 0 {
			be.MessageAttributes = map[string]types.MessageAttributeValue{}
			for k, v := range e.Attrs {
				be.MessageAttributes[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
			}
		}
		in.Entries = append(in.Entries, be)
	}
	out, err := p.client.SendMessageBatch(ctx, in)
	if err != nil {
		return nil, Classify(err)
	}
	failed := map[string]error{}
	for _, f := range out.Failed {
		failed[aws.ToString(f.Id)] = fmt.Errorf("%s: %s", aws.ToString(f.Code), aws.ToString(f.Message))
	}
	return failed, nil
}

// Classify marks network failures, throttling and 5xx answers as transient.
func Classify(err error) error {
	if err == nil {
		return nil
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", app.ErrTransient, err)
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ThrottlingException", "RequestThrottled", "ServiceUnavailable", "InternalError", "InternalFailure",
			"KmsThrottled", "RequestTimeout", "ProvisionedThroughputExceededException":
			return fmt.Errorf("%w: %w", app.ErrTransient, err)
		}
		if apiErr.ErrorFault() == smithy.FaultServer {
			return fmt.Errorf("%w: %w", app.ErrTransient, err)
		}
		return err
	}
	// Unknown transport failures (connection refused while resolving, etc.).
	return fmt.Errorf("%w: %w", app.ErrTransient, err)
}
