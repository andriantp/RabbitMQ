module example.com/rabbitmq/amqp091

go 1.23

require (
	example.com/rabbitmq/reconnect v0.0.0
	github.com/rabbitmq/amqp091-go v1.17.0
)

replace example.com/rabbitmq/reconnect => ../reconnect
