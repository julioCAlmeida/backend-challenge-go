package worker

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
	"github.com/julioCAlmeida/backend-challenge-go/internal/contract"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/derr"
	"github.com/julioCAlmeida/backend-challenge-go/internal/domain/events"
	"github.com/julioCAlmeida/backend-challenge-go/internal/faults"
	"github.com/julioCAlmeida/backend-challenge-go/internal/infra/sqsx"
	"github.com/julioCAlmeida/backend-challenge-go/internal/observability"
)

// ConsumerConfig tunes the SQS consumer.
type ConsumerConfig struct {
	Name              string
	QueueURL          string
	DLQURL            string
	Workers           int
	WaitTime          time.Duration
	VisibilityTimeout time.Duration
	ProcessingTimeout time.Duration
	MaxReceiveCount   int
	BackoffBase       time.Duration
	BackoffMax        time.Duration
}

// Consumer reads wager-transactions.fifo and processes each message through the
// same use case as HTTP. A message is deleted only after the SQL transaction
// that registered it in the inbox and applied it has committed.
//
// Outcomes:
//   - processed / business rejection / pending reference / duplicate: delete;
//   - invalid message, payload conflict, unknown wallet: sent to the DLQ explicitly
//     and deleted (retrying cannot fix them);
//   - transient failure or unexpected error: visibility set to an exponential
//     backoff; after MaxReceiveCount receives SQS moves it to the DLQ (redrive).
type Consumer struct {
	client  *sqs.Client
	svc     *app.WageringService
	cfg     ConsumerConfig
	log     *slog.Logger
	metrics *observability.Metrics
	faults  *faults.Injector
	runner  *Runner
}

func NewConsumer(client *sqs.Client, svc *app.WageringService, cfg ConsumerConfig, log *slog.Logger, m *observability.Metrics, f *faults.Injector) *Consumer {
	return &Consumer{client: client, svc: svc, cfg: cfg, log: log.With(slog.String("worker", "sqs-consumer")), metrics: m, faults: f,
		runner: NewRunner("sqs-consumer", log)}
}

// SetQueues sets the queue URLs resolved at startup.
func (c *Consumer) SetQueues(queueURL, dlqURL string) {
	c.cfg.QueueURL, c.cfg.DLQURL = queueURL, dlqURL
}

func (c *Consumer) Start() {
	c.runner.Go(c.cfg.Workers, func(ctx context.Context, _ int) { c.loop(ctx) })
}

// Stop stops receiving, lets in-flight messages finish within the processing
// timeout and releases the visibility of messages not started.
func (c *Consumer) Stop(ctx context.Context) error { return c.runner.Stop(ctx) }

func (c *Consumer) loop(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.cfg.QueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     int32(c.cfg.WaitTime.Seconds()),
			VisibilityTimeout:   int32(c.cfg.VisibilityTimeout.Seconds()),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount, types.MessageSystemAttributeNameMessageGroupId,
			},
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			c.log.WarnContext(ctx, "receive failed", slog.Any("error", err))
			sleep(ctx, app.Backoff(500*time.Millisecond, 15*time.Second, failures))
			continue
		}
		failures = 0
		blockedGroups := map[string]bool{}
		for i, msg := range out.Messages {
			if ctx.Err() != nil {
				c.release(out.Messages[i:])
				return
			}
			group := msg.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
			if blockedGroups[group] {
				// Keep FIFO order: an earlier message of this group was not completed.
				c.release([]types.Message{msg})
				continue
			}
			if !c.handle(ctx, msg) {
				blockedGroups[group] = true
			}
		}
	}
}

// handle returns false when the message stays in the queue.
func (c *Consumer) handle(loopCtx context.Context, msg types.Message) bool {
	// In-flight work is not canceled by shutdown; it gets its own deadline.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(loopCtx), c.cfg.ProcessingTimeout)
	defer cancel()
	receiveCount, _ := strconv.Atoi(msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	ctx = observability.WithLogAttrs(ctx, slog.String("sqsMessageId", aws.ToString(msg.MessageId)), slog.Int("receiveCount", receiveCount))

	envelope, req, hash, err := contract.ParseWagerMessage([]byte(aws.ToString(msg.Body)))
	if err != nil {
		c.log.WarnContext(ctx, "invalid message", slog.String("code", derr.CodeOf(err)), slog.Any("error", err))
		return c.deadLetter(ctx, msg, "invalid-message", derr.CodeOf(err))
	}
	correlationID := envelope.MessageID
	if v, ok := msg.MessageAttributes["correlationId"]; ok && aws.ToString(v.StringValue) != "" {
		correlationID = aws.ToString(v.StringValue)
	}
	ctx = observability.WithLogAttrs(ctx, slog.String("messageId", envelope.MessageID), slog.String("correlationId", correlationID),
		slog.String("providerId", req.ProviderID), slog.String("walletId", req.WalletID.String()))

	res, err := c.svc.Process(ctx, app.ProcessCommand{
		Request: req, Channel: app.ChannelSQS,
		Meta:  events.Meta{CorrelationID: correlationID, CausationID: envelope.MessageID},
		Inbox: &app.InboxMessage{Consumer: c.cfg.Name, MessageID: envelope.MessageID, PayloadHash: hash, ReceivedAt: time.Now().UTC().Truncate(time.Microsecond)},
	})
	switch {
	case err == nil:
		c.faults.Crash(faults.ConsumerAfterCommit)
		outcome := string(res.Transaction.Status)
		if res.DuplicateMessage {
			outcome = "DUPLICATE"
		}
		c.metrics.SQSMessages.WithLabelValues(outcome).Inc()
		return c.delete(ctx, msg)
	case isPermanent(err):
		c.log.WarnContext(ctx, "message rejected permanently", slog.String("code", derr.CodeOf(err)), slog.Any("error", err))
		return c.deadLetter(ctx, msg, "permanent-error", derr.CodeOf(err))
	default:
		c.metrics.Retry("sqs-consumer")
		c.metrics.SQSMessages.WithLabelValues("RETRY").Inc()
		delay := app.Backoff(c.cfg.BackoffBase, c.cfg.BackoffMax, receiveCount-1)
		if loopCtx.Err() != nil {
			delay = 0 // shutting down: hand the message to another instance now
		}
		c.log.WarnContext(ctx, "message processing failed, will retry",
			slog.Any("error", err), slog.Duration("retryIn", delay), slog.Int("maxReceiveCount", c.cfg.MaxReceiveCount))
		c.changeVisibility(ctx, msg, delay)
		return false
	}
}

// isPermanent: errors that a redelivery cannot fix.
func isPermanent(err error) bool {
	if app.IsRetryable(err) {
		return false
	}
	switch derr.KindOf(err) {
	case derr.KindValidation, derr.KindConflict, derr.KindNotFound:
		return true
	}
	return false
}

func (c *Consumer) delete(ctx context.Context, msg types.Message) bool {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: msg.ReceiptHandle})
	if err != nil {
		// The work is committed; a redelivery will be answered from the inbox.
		c.log.WarnContext(ctx, "delete failed; message will be redelivered and deduplicated", slog.Any("error", err))
	}
	return true
}

func (c *Consumer) deadLetter(ctx context.Context, msg types.Message, reason, code string) bool {
	group := msg.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "invalid"
	}
	attrs := map[string]string{"failureReason": reason, "originalMessageId": aws.ToString(msg.MessageId)}
	if code != "" {
		attrs["failureCode"] = code
	}
	err := sqsx.NewPublisher(c.client, c.cfg.DLQURL).Publish(ctx, group, aws.ToString(msg.MessageId), aws.ToString(msg.Body), attrs)
	if err != nil {
		c.log.ErrorContext(ctx, "could not send message to DLQ; will retry", slog.Any("error", err))
		c.changeVisibility(ctx, msg, c.cfg.BackoffBase)
		return false
	}
	c.metrics.DLQ.WithLabelValues(reason).Inc()
	c.metrics.SQSMessages.WithLabelValues("DLQ").Inc()
	return c.delete(ctx, msg)
}

func (c *Consumer) changeVisibility(ctx context.Context, msg types.Message, d time.Duration) {
	secs := int32(d.Seconds())
	if secs > 43200 {
		secs = 43200
	}
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: msg.ReceiptHandle, VisibilityTimeout: secs,
	})
	if err != nil {
		c.log.WarnContext(ctx, "change visibility failed; visibility timeout applies", slog.Any("error", err))
	}
}

// release makes messages that were received but not processed visible again.
func (c *Consumer) release(msgs []types.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, m := range msgs {
		c.changeVisibility(ctx, m, 0)
	}
}
