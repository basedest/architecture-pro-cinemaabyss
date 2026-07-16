package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Kafka topics. These MUST match KAFKA_CREATE_TOPICS in docker-compose.yml.
const (
	topicMovie   = "movie-events"
	topicUser    = "user-events"
	topicPayment = "payment-events"
)

const consumerGroup = "events-service"

// writeResult carries the broker-assigned coordinates back from the async
// completion callback via kafka.Message.WriterData.
type writeResult struct {
	partition int
	offset    int64
	err       error
}

// producer owns one writer per topic.
type producer struct {
	writers map[string]*kafka.Writer
}

func newProducer(brokers []string) *producer {
	p := &producer{writers: make(map[string]*kafka.Writer)}
	for _, t := range []string{topicMovie, topicUser, topicPayment} {
		p.writers[t] = &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  t,
			Balancer:               &kafka.LeastBytes{},
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
			Completion:             onCompletion,
		}
	}
	return p
}

// onCompletion fans the broker result back into each message's WriterData
// channel so produce() can report the real partition/offset synchronously.
func onCompletion(messages []kafka.Message, err error) {
	for _, m := range messages {
		if ch, ok := m.WriterData.(chan writeResult); ok {
			ch <- writeResult{partition: m.Partition, offset: m.Offset, err: err}
		}
	}
}

// produce writes a single message and blocks until the broker acknowledges it
// (or a 10s timeout elapses), returning the assigned partition and offset.
func (p *producer) produce(ctx context.Context, topic string, value []byte) (int, int64, error) {
	w, ok := p.writers[topic]
	if !ok {
		return 0, 0, errors.New("unknown topic: " + topic)
	}

	ch := make(chan writeResult, 1)
	msg := kafka.Message{Value: value, WriterData: ch}

	if err := w.WriteMessages(ctx, msg); err != nil {
		return 0, 0, err
	}

	select {
	case res := <-ch:
		if res.err != nil {
			return 0, 0, res.err
		}
		return res.partition, res.offset, nil
	case <-time.After(10 * time.Second):
		return 0, 0, errors.New("timed out waiting for kafka write acknowledgement")
	}
}

func (p *producer) close() {
	for _, w := range p.writers {
		_ = w.Close()
	}
}

// startConsumers launches one reader goroutine per topic. Each consumed message
// is logged — this log is the Task 2 Kafka consumer deliverable.
func startConsumers(ctx context.Context, brokers []string) {
	for _, t := range []string{topicMovie, topicUser, topicPayment} {
		go consume(ctx, brokers, t)
	}
}

func consume(ctx context.Context, brokers []string, topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: consumerGroup,
		Topic:   topic,
	})
	defer r.Close()

	log.Printf("events: consumer started for topic %s", topic)

	for {
		if ctx.Err() != nil {
			return
		}
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Kafka boots slower than this service; back off and retry.
			log.Printf("events: read error on topic %s: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("Processing %s: partition=%d offset=%d value=%s",
			topic, m.Partition, m.Offset, string(m.Value))
	}
}
