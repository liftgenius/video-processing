import pika



# credentials = pika.PlainCredentials(username, password)
# parameters = pika.ConnectionParameters(host, credentials=credentials, heartbeat=300)

def get_plain_credentials(username='guest', password='guest'):
    return pika.PlainCredentials(str(username), str(password))

def get_parameters(credentials: pika.PlainCredentials, host='0.0.0.0', heartbeat=120):
    return pika.ConnectionParameters(host, credentials=credentials, heartbeat=heartbeat)

def ack_message(ch, delivery_tag):
    """Note that `ch` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if ch.is_open:
        ch.basic_ack(delivery_tag)
        print(' \n 📧 Waiting for messages. To exit press CTRL+C')
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass

def send_message(parameters: pika.ConnectionParameters, exchange_name: str, queue_name: str, routing_key_name: str, message: str, quiet=True):
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type="direct",
        passive=False,
        durable=True,
        auto_delete=False)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        queue=queue_name, 
        exchange=exchange_name, 
        routing_key=routing_key_name
        )
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=routing_key_name,
        body=message,
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
    )
    if not quiet:
        print(f" ✈️ Sent {message} to {exchange_name}")
    connection.close()

def setup_channel(parameters: pika.ConnectionParameters, exchange_name: str, queue_name: str, routing_key_name: str):
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type="direct",
        passive=False,
        durable=True,
        auto_delete=False)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        queue=queue_name, 
        exchange=exchange_name, 
        routing_key=routing_key_name
        )
    return connection, channel

def connect_to_channel(parameters: pika.ConnectionParameters, exchange_name: str, queue_name: str, routing_key_name: str):
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_bind(
        queue=queue_name, 
        exchange=exchange_name, 
        routing_key=routing_key_name
        )
    return connection, channel