
import puka
import random


class Pats(puka.Client):
    exchange = 'pats'

    def x_connect(self):
        t = self.connect()
        self.wait(t)
        t = self.exchange_declare(exchange=self.exchange, type='topic')
        self.wait(t)
        t = self.queue_declare(queue='', auto_delete=True, exclusive=True)
        self.resp_qname = self.wait(t)['queue']
        self.resp_map = {}
        self.basic_consume(self.resp_qname, no_ack=True,
                           callback=self.on_response)

    def on_response(self, t, result):
        corr_id = result['headers']['correlation_id']
        if corr_id in self.resp_map:
            fun = self.resp_map[corr_id]
            if fun:
                fun(result['body'])
            del self.resp_map[corr_id]

    def request(self, topic, data='', callback=None):
        corr_id = str(int(random.random()*1E14))
        self.resp_map[corr_id] = callback
        self.basic_publish(exchange=self.exchange, routing_key=topic, body=data,
                           headers={'reply_to': self.resp_qname,
                                    'correlation_id': corr_id,
                                    'persistent': False})

    def subscribe(self, topic, callback=None):
        t = self.queue_declare(queue='', auto_delete=True, exclusive=True)
        result = self.wait(t)
        qname = result['queue']
        t = self.queue_bind(exchange=self.exchange, queue=qname,
                            binding_key=topic)
        self.wait(t)

        def on_delivery(t, result):
            reply_to = (result['headers'].get('reply_to'),
                        result['headers'].get('correlation_id'))
            callback(result['body'], reply_to, result['routing_key'])
        self.basic_consume(queue=qname, no_ack=True,
                           callback=on_delivery)

    def publish(self, topic, data='', callback=None):
        if isinstance(topic, tuple):
            return self.reply(topic, data, callback)

        def on_basic_publish(t, result):
            callback()
        if not callback:
            self.basic_publish(exchange=self.exchange, routing_key=topic,
                                     body=data)
        else:
            self.basic_publish(exchange=self.exchange, routing_key=topic,
                               body=data, callback=on_basic_publish)

    def reply(self, reply_to, data, callback=None):
        qname, corr_id = reply_to
        self.basic_publish(exchange='', routing_key=qname,
                           headers={'correlation_id': corr_id,
                                    'persistent': False}, body=data)
