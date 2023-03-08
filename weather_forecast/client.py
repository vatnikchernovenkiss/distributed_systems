import pika
import uuid
import sys


class Client(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue = result.method.queue
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=True)
        
    def callback(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, msg):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='', routing_key=routing_key,
                                   properties=pika.BasicProperties(reply_to=self.queue, correlation_id=self.corr_id),
                                   body=msg)
        while self.response is None:
            self.connection.process_data_events()

        return self.response


if __name__ == '__main__':
    routing_key = 'rcc_queue'
    client_rpc = Client()
    print('minimum available date - 2019-04-25; maximum available date - 2022-01-01')
    while True:
        input_msg = input ("Enter first and last date and time delta (in minutes): ")
        if input_msg == 'exit':
            break
        response = client_rpc.call(input_msg)
        print(response.decode('utf-8'))