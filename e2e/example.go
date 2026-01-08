package main

import (
	"context"
	"sync"
	"time"

	"github.com/upfluence/amqp"
	"github.com/upfluence/amqp/amqputil"
	"github.com/upfluence/amqp/backend"
	"github.com/upfluence/amqp/consumer"
	"github.com/upfluence/pkg/v2/log"
)

func main() {
	var (
		wg sync.WaitGroup

		b   = amqputil.Open().(*backend.Broker)
		ctx = context.Background()

		c, err = consumer.BuildConsumer(ctx, b, amqp.ConsumeOptions{})
	)

	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	wg.Add(3)

	go func() {
		defer wg.Done()

		for {
			stats := b.Stats()

			log.Noticef("broker stats: open: %v, idle: %d, used: %d, consuming: %d", stats.ConnectionOpened, stats.IdleChannel, stats.InUseChannel, stats.ConsumingChannel)

			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		defer wg.Done()

		for {
			if c != nil {
				if err := b.Publish(
					ctx,
					"",
					c.QueueName(),
					amqp.Message{
						Body:        []byte("Hello, World!"),
						ContentType: "text/plain",
					},
					amqp.PublishOptions{},
				); err != nil {
					log.Errorf("failed to publish message: %v", err)
				}
			}
		}
	}()

	go func() {
		defer wg.Done()

		for {
			if c == nil || !c.IsOpen() {
				c, err = consumer.BuildConsumer(ctx, b, amqp.ConsumeOptions{})

				if err != nil {
					log.Errorf("failed to create consumer: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
			}

			for {
				msg, err := c.Next(ctx)
				if err != nil {
					log.Errorf("failed to get next message: %v", err)
					time.Sleep(5 * time.Second)
					break
				}

				if err := c.Ack(ctx, msg.DeliveryTag, amqp.AckOptions{}); err != nil {
					log.Errorf("failed to ack message: %v", err)
				}
			}
		}
	}()

	wg.Wait()

}
