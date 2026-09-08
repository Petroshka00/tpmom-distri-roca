package factory

import (
	"context"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQQueueMiddleware struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
}

func (r *RabbitMQQueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	deliveries, err := r.ch.Consume(r.queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for d := range deliveries {
		delivery := d
		ack := func() {
			_ = delivery.Ack(false)
		}
		nack := func() {
			_ = delivery.Nack(false, true)
		}

		callbackFunc(m.Message{Body: string(delivery.Body)}, ack, nack)
	}

	return nil
}

func (r *RabbitMQQueueMiddleware) StopConsuming() error {
	if r.ch != nil {
		_ = r.ch.Cancel("", false)
	}
	return nil
}

func (r *RabbitMQQueueMiddleware) Send(msg m.Message) error {
	return r.ch.PublishWithContext(context.Background(), "", r.queueName, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(msg.Body),
	})
}

func (r *RabbitMQQueueMiddleware) Close() error {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("Connection error: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Channel error: %w", err)
	}

	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("Queue declare error: %w", err)
	}
	return &RabbitMQQueueMiddleware{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
	}, nil
}

type RabbitMQExchangeMiddleware struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	exchangeName string
	routingKeys  []string
}

func (r *RabbitMQExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	q, err := r.ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return err
	}

	for _, key := range r.routingKeys {
		err = r.ch.QueueBind(
			q.Name,
			key,
			r.exchangeName,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	deliveries, err := r.ch.Consume(q.Name, "", false, false, false, false, nil)

	if err != nil {
		return err
	}

	for d := range deliveries {
		delivery := d
		ack := func() {
			_ = delivery.Ack(false)
		}
		nack := func() {
			_ = delivery.Nack(false, true)
		}

		callbackFunc(m.Message{Body: string(delivery.Body)}, ack, nack)
	}

	return nil
}

func (r *RabbitMQExchangeMiddleware) StopConsuming() error {
	if r.ch != nil {
		_ = r.ch.Cancel("", false)
	}
	return nil
}

func (r *RabbitMQExchangeMiddleware) Send(msg m.Message) error {
	keys := r.routingKeys
	if len(keys) == 0 {
		keys = []string{""}
	}

	for _, key := range keys {
		err := r.ch.PublishWithContext(
			context.Background(),
			r.exchangeName,
			key,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(msg.Body),
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RabbitMQExchangeMiddleware) Close() error {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("Connection error: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("Channel error: %w", err)
	}

	err = ch.ExchangeDeclare(exchange, "direct", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("Exchange declare error: %w", err)
	}

	return &RabbitMQExchangeMiddleware{
		conn:         conn,
		ch:           ch,
		exchangeName: exchange,
		routingKeys:  keys,
	}, nil
}
