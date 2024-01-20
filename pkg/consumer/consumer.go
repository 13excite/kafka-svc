package consumer

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type Consumer interface {
	Health(context.Context) (interface{}, error)
	Consume(context.Context) error
	Close() error
}

type Callback func(ctx context.Context, msg []byte) error

type consumer struct {
	client    sarama.Client
	reader    sarama.ConsumerGroup
	callback  Callback
	topicFrom string
}

func New(ctx context.Context, addrs []string, group, topicFrom string, callback Callback) (Consumer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		return nil, fmt.Errorf("could not create kafka consumer client: %v", err)
	}

	reader, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		return nil, fmt.Errorf("could not create kafka consumer group: %v", err)
	}

	p := &consumer{
		client:    client,
		reader:    reader,
		topicFrom: topicFrom,
		callback:  callback,
	}

	return p, nil
}

func (p *consumer) Consume(ctx context.Context) error {
	for ctx.Err() == nil {
		// `Consume` should be called inside an infinite loop, when
		// a Kafka rebalance happens, the consumer session will need to be
		// recreated for getting the new claims
		if err := p.reader.Consume(ctx, []string{p.topicFrom}, p); err != nil {
			return fmt.Errorf("error from consumer: %v", err)
		}
	}

	return ctx.Err()
}

func (p *consumer) Setup(s sarama.ConsumerGroupSession) error {
	// TODO: switch to logger and set debug level
	log.Println("setting up kafka consumer group")

	return nil
}

func (p *consumer) Cleanup(s sarama.ConsumerGroupSession) error {
	// TODO: switch to logger and set debug level
	log.Println("cleaning up kafka consumer group")

	return nil
}

func (p *consumer) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		err := p.callback(s.Context(), msg.Value)
		if err != nil {
			s.MarkMessage(msg, "error")
			continue
		}
		// mark message as consumed
		s.MarkMessage(msg, "")
	}

	return nil
}

func (p *consumer) Health(ctx context.Context) (interface{}, error) {
	brokers := p.client.Brokers()

	if len(brokers) == 0 {
		return nil, fmt.Errorf("no active kafka brokers")
	}

	if p.client.Closed() {
		return nil, fmt.Errorf("kafka client is closed")
	}

	return map[string]interface{}{
		"brokers": len(brokers),
	}, nil
}

func (p *consumer) Close() error {
	err := p.reader.Close()
	if err != nil {
		log.Println("failed to close kafka reader: ", err)
	}

	return nil
}
