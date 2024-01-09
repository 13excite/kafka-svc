package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// fnError is a function that handles errors from the producer
type fnError func(string, error)

// TODO: add metrics, logging, tracing
type Producer interface {
	Health(context.Context) (interface{}, error)
	Dump(ctx context.Context, topic, key string, msgs ...[]byte) error
	Close() error
}

type producer struct {
	client  sarama.Client
	writer  sarama.AsyncProducer
	fnError fnError
}

func New(ctx context.Context, addrs []string, fnError fnError, opts ...func(*sarama.Config)) (Producer, error) {
	config := sarama.NewConfig()
	// wait for all in-sync replicas to ack the message
	config.Producer.Return.Successes = true

	// apply options
	for _, opt := range opts {
		opt(config)
	}

	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		return nil, fmt.Errorf("could not create client: %v", err)
	}

	writer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("could not create async producer: %v", err)
	}
	// close by context cancellation
	go func() {
		<-ctx.Done()
		writer.AsyncClose()
	}()

	p := &producer{
		client:  client,
		writer:  writer,
		fnError: fnError,
	}

	// handle errors
	go func() {
		for producerError := range writer.Errors() {
			if p.fnError != nil {
				p.fnError(producerError.Msg.Topic, producerError.Err)
			}
		}
	}()

	return p, nil
}

func (p *producer) Health(ctx context.Context) (interface{}, error) {
	brokers := p.client.Brokers()

	if len(brokers) == 0 {
		return nil, fmt.Errorf("no active brokers")
	}
	if p.client.Closed() {
		return nil, fmt.Errorf("kafka client is closed")
	}
	return map[string]interface{}{
		"brokers": len(brokers),
	}, nil
}

func (p *producer) Dump(ctx context.Context, topic, stringKey string, msgs ...[]byte) error {
	timestamp := time.Time{}
	// make sure we have a key. If key is an empty string the ProducerMessage.Key will be nil
	var key sarama.Encoder
	if stringKey != "" {
		key = sarama.StringEncoder(stringKey)
	}

	for _, msg := range msgs {
		select {
		case p.writer.Input() <- &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.ByteEncoder(msg),
			Key:       key,
			Timestamp: timestamp,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (p *producer) Close() error {
	return p.writer.Close()
}
