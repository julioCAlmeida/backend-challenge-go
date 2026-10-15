package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/julioCAlmeida/backend-challenge-go/internal/app"
)

type inboxRepo struct{ q querier }

// Register inserts (consumer, messageId). A concurrent delivery of the same
// message blocks on the primary key until the first one commits or aborts.
func (r inboxRepo) Register(ctx context.Context, m app.InboxMessage) (*app.InboxRecord, error) {
	tag, err := r.q.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		m.Consumer, m.MessageID, m.PayloadHash, m.ReceivedAt)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	rec := &app.InboxRecord{}
	var outcome *string
	err = r.q.QueryRow(ctx, `SELECT consumer_name, message_id, payload_hash, received_at, transaction_id, outcome, completed_at
		FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, m.Consumer, m.MessageID).
		Scan(&rec.Consumer, &rec.MessageID, &rec.PayloadHash, &rec.ReceivedAt, &rec.TransactionID, &outcome, &rec.CompletedAt)
	if err != nil {
		return nil, err
	}
	rec.Outcome = deref(outcome)
	return rec, nil
}

func (r inboxRepo) Complete(ctx context.Context, consumer, messageID string, transactionID *uuid.UUID, outcome string, at time.Time) error {
	_, err := r.q.Exec(ctx, `UPDATE inbox_messages SET transaction_id = $3, outcome = $4, completed_at = $5
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID, transactionID, outcome, at)
	return err
}
