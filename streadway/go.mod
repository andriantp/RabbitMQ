module example.com/rabbitmq/streadway

go 1.23

require (
	example.com/rabbitmq/reconnect v0.0.0
	github.com/streadway/amqp v1.1.0
)

replace example.com/rabbitmq/reconnect => ../reconnect
