package jtsbr

import "context"

type Broker struct{}

func New() *Broker {
	return &Broker{}
}

func (b *Broker) Start(map[string]any) error {

	return nil
}

func (b *Broker) Close() error {

	return nil
}

func (b *Broker) Produce(
	ctx context.Context,
	channel string,
	queue string,
	object any,
) error {

	return nil
}

func (b *Broker) Consume(
	ctx context.Context,
	channel string,
	queue string,
	action func(context.Context, []byte),
) error {

	return nil
}
