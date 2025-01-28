package jtsbr

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Broker struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

func New() *Broker {
	return &Broker{}
}

func (b *Broker) Start(params map[string]any) error {
	url, ok := params["url"].(string)
	if !ok {
		return fmt.Errorf("invalid params")
	}

	conn, err := nats.Connect(url)
	if err != nil {
		return fmt.Errorf("start, nats connect: %v", err)
	}
	b.conn = conn

	js, err := jetstream.New(conn)
	if err != nil {
		return fmt.Errorf("start, new jet stream: %v", err)
	}
	b.js = js

	return nil
}

func (b *Broker) Close() error {
	b.conn.Close()

	return nil
}

func (b *Broker) Produce(
	ctx context.Context,
	channel string,
	queue string,
	object any,
) error {
	if _, err := b.addStreamIfNeeded(ctx, channel); err != nil {
		return fmt.Errorf("produce, add stream if needed: %v", err)
	}

	body, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("produce, marshal: %v", err)
	}

	if _, err := b.js.Publish(ctx, fmt.Sprintf("%s.%s", channel, queue), body); err != nil {
		return fmt.Errorf("produce, publish: %v", err)
	}

	return nil
}

func (b *Broker) Consume(
	ctx context.Context,
	channel string,
	queue string,
	action func(context.Context, []byte),
) error {
	s, err := b.addStreamIfNeeded(ctx, channel)
	if err != nil {
		return fmt.Errorf("consume, add stream if needed: %v", err)
	}

	c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "CONS",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("consume, create or update consumer: %v", err)
	}

	go b.listenQueue(ctx, c, action)

	return nil
}

func (b *Broker) addStreamIfNeeded(ctx context.Context, channel string) (jetstream.Stream, error) {
	return b.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     channel,
		Subjects: []string{fmt.Sprintf("%s.*", channel)},
	})
}

func (b *Broker) listenQueue(
	ctx context.Context,
	consume jetstream.Consumer,
	block func(context.Context, []byte),
) error {
	msgs, err := consume.Messages()
	if err != nil {
		return fmt.Errorf("listen queue, messages: %v", err)
	}
	defer msgs.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := msgs.Next()
			if err != nil {
				log.Println("listen queue, next error:", err)
				continue
			}

			block(ctx, msg.Data())
		}
	}
}
