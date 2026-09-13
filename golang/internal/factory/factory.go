package factory

import (
	"context"
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQQueueMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	queueName   string
	consumerTag string
	consumerSeq int
	isConsuming bool
	manualStop  bool
}

func (r *RabbitMQQueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	if r.isConsuming {
		return m.ErrMessageMiddlewareMessage
	}

	r.consumerSeq++
	tag := fmt.Sprintf("q-cons-%s-%d", r.queueName, r.consumerSeq)
	r.consumerTag = tag
	r.isConsuming = true
	r.manualStop = false

	deliveries, err := r.ch.Consume(
		r.queueName,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.isConsuming = false
		r.consumerTag = ""
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
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

	wasManual := r.manualStop
	r.isConsuming = false
	r.consumerTag = ""

	if wasManual {
		return nil
	}

	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return m.ErrMessageMiddlewareMessage
}

func (r *RabbitMQQueueMiddleware) StopConsuming() error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	if !r.isConsuming || r.consumerTag == "" {
		return nil
	}

	r.manualStop = true
	err := r.ch.Cancel(r.consumerTag, false)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	return nil
}

func (r *RabbitMQQueueMiddleware) Send(msg m.Message) error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	err := r.ch.PublishWithContext(context.Background(), "", r.queueName, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(msg.Body),
	})
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (r *RabbitMQQueueMiddleware) Close() error {
	var closeErr error
	if r.ch != nil && !r.ch.IsClosed() {
		if err := r.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = m.ErrMessageMiddlewareClose
		}
	}
	if r.conn != nil && !r.conn.IsClosed() {
		if err := r.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = m.ErrMessageMiddlewareClose
		}
	}
	return closeErr
}

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, m.ErrMessageMiddlewareMessage
	}

	_ = ch.Qos(1, 0, false)

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
	queueName    string
	consumerTag  string
	consumerSeq  int
	isConsuming  bool
	manualStop   bool
}

func (r *RabbitMQExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	if r.isConsuming {
		return m.ErrMessageMiddlewareMessage
	}

	q, err := r.ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	r.queueName = q.Name

	keys := r.routingKeys
	if len(keys) == 0 {
		keys = []string{""}
	}

	for _, key := range keys {
		err = r.ch.QueueBind(
			q.Name,
			key,
			r.exchangeName,
			false,
			nil,
		)
		if err != nil {
			if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}

	r.consumerSeq++
	tag := fmt.Sprintf("ex-cons-%s-%d", r.exchangeName, r.consumerSeq)
	r.consumerTag = tag
	r.isConsuming = true
	r.manualStop = false

	deliveries, err := r.ch.Consume(
		q.Name,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		r.isConsuming = false
		r.consumerTag = ""
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
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

	wasManual := r.manualStop
	r.isConsuming = false
	r.consumerTag = ""

	if wasManual {
		return nil
	}

	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return m.ErrMessageMiddlewareMessage
}

func (r *RabbitMQExchangeMiddleware) StopConsuming() error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	if !r.isConsuming || r.consumerTag == "" {
		return nil
	}

	r.manualStop = true
	err := r.ch.Cancel(r.consumerTag, false)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	return nil
}

func (r *RabbitMQExchangeMiddleware) Send(msg m.Message) error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

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
			if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (r *RabbitMQExchangeMiddleware) Close() error {
	var closeErr error
	if r.ch != nil && !r.ch.IsClosed() {
		if err := r.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = m.ErrMessageMiddlewareClose
		}
	}
	if r.conn != nil && !r.conn.IsClosed() {
		if err := r.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = m.ErrMessageMiddlewareClose
		}
	}
	return closeErr
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	err = ch.ExchangeDeclare(exchange, "direct", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, m.ErrMessageMiddlewareMessage
	}

	return &RabbitMQExchangeMiddleware{
		conn:         conn,
		ch:           ch,
		exchangeName: exchange,
		routingKeys:  keys,
	}, nil
}
