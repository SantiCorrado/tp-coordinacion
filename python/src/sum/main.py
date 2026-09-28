import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
#Estos serian los 2 tipos de mensajes enviados por el exchange de control
RECV_EOF = 1 #indica que uno de los sum obtuvo el eof de uno de los clientes
REPORT_MSG  = 2 #indica que uno de los clientes recibio uno de los ultimos mensajes

#BASADO EN https://dev.to/ali_algmass/hash-functions-determinism-a-deep-dive-a9g
def aggregator_responsable(client_id):
    hash = 7
    for c in client_id:
        hash = hash * 31 + ord(c)
    return hash % AGGREGATION_AMOUNT

def fill_set(eof_msg_id)-> set:
    res = set()
    i = 1
    while i <= SUM_AMOUNT and (eof_msg_id-i) >= 0:
        res.add(eof_msg_id-i)
        i += 1
    return res

class Connection:
    def __init__(self, id):
        self.fruits ={}
        self.id = id
        self.msg_ids = [] #ids de mensajes recibidos por el gateway
        self.msg_set = set() #ultimos mensajes esperados
        self.msg_recv = set() #ids de ultimos mensajes recibidos por otros sum
        self.recv_eof = False
        self.result_sent = False

    def add_fruit(self, fruit, amount):
        fi = fruit_item.FruitItem(fruit, int(amount))
        if fruit not in self.fruits:
            self.fruits[fruit] = fi
        else:
            self.fruits[fruit] = self.fruits[fruit] + fi 

class SumFilter:
        
    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 3:
            logging.error(f"Invalid control message received: {fields}")
            nack()
            return
        [msg_type, client_id, msg_id] = fields
        with self.client_acces:
            if client_id not in self.client:
                self.client[client_id] = Connection(client_id)
        result = None
        if msg_type == RECV_EOF:
            report = []
            with self.client_acces:
                conn = self.client[client_id]
                conn.msg_set = fill_set(msg_id)
                conn.recv_eof = True
                for n in conn.msg_recv:
                    if n in conn.msg_set:
                        conn.msg_set.remove(n)
                conn.msg_recv.clear()
                for e in conn.msg_ids[(len(conn.msg_ids)-SUM_AMOUNT):]:
                    if e in conn.msg_set:
                        conn.msg_set.remove(e)
                        logging.info(
                            f"SUM {ID}: received final msg "
                            f"client={client_id}, msgid={e}"
                        )
                        report.append(e)
                if len(conn.msg_set) == 0 and not conn.result_sent:
                    result = []
                    conn.result_sent = True
                    for fr in conn.fruits.values():
                        result.append([fr.fruit , fr.amount])
            for r in report:
                self.sum_control_exchange.send(message_protocol.internal.serialize([REPORT_MSG , client_id, r]))             
        elif msg_type == REPORT_MSG :
            with self.client_acces:
                conn = self.client[client_id]
                if msg_id in conn.msg_set:
                    conn.msg_set.remove(msg_id)
                elif not conn.recv_eof:
                    conn.msg_recv.add(msg_id)
                if len(conn.msg_set) == 0 and not conn.result_sent and conn.recv_eof:
                    result = []
                    conn.result_sent = True
                    for fr in conn.fruits.values():
                        result.append([fr.fruit , fr.amount])
        else:
            logging.error(f"Invalid control message type received: {msg_type}")
            nack()
            return 
        if result is not None:
            message = [client_id, ID, result]
            self.data_output_exchanges[aggregator_responsable(client_id)].send(message_protocol.internal.serialize(message))
        ack()

    def handle_control_exchange(self):
        logging.info(f"Starting control exchange")
        self.sum_control_exchange.start_consuming(self.process_control_message)

    def __init__(self):
        self.client = {} #En este diccioario aparecen los clientes que ya enviaron su EOF con id de msg
        self.client_acces = threading.Lock()
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.sum_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        self.sum_control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_CONTROL_EXCHANGE]
        )
        self.control_thread = threading.Thread(target=self.handle_control_exchange)
        self.control_thread.start()

    def _process_data(self, id, msgid, fruit, amount):
        logging.info(f"Process data")
        logging.info(
            f"Sum {ID}: received client={id} msg_id={msgid}"
        )
        result = None
        report = None
        with self.client_acces:
            if id not in self.client:
                self.client[id] = Connection(id)
            conn = self.client[id]
            if len(conn.msg_ids) != 0:
                if (int(conn.msg_ids[len(conn.msg_ids)-1]) + 1) == msgid:
                    logging.info(
                        f"SUM {ID}: received CONSECUTIVE msg {conn.msg_ids[-1]} - {msgid}"
                        f"client={id}, msgid={report}"
                    )
            conn.add_fruit(fruit,amount)
            conn.msg_ids.append(msgid)
            conn = self.client[id]
            if msgid in conn.msg_set:
                conn.msg_set.remove(msgid)
                report = msgid
                if len(conn.msg_set) == 0 and not conn.result_sent and conn.recv_eof:
                    result = []
                    conn.result_sent = True
                    for fr in conn.fruits.values():
                        result.append([fr.fruit , fr.amount])
        if report is not None:
            logging.info(
                f"SUM {ID}: received final msg "
                f"client={id}, msgid={report}"
            )
            self.sum_control_output_exchange.send(message_protocol.internal.serialize([REPORT_MSG , id, report]))
        if result is not None:
            message = [id, ID, result]
            self.data_output_exchanges[aggregator_responsable(id)].send(message_protocol.internal.serialize(message))

    def _process_eof(self, client_id, msgid):
        logging.info(f"Broadcasting data messages")
        logging.info(
            f"Sum {ID}: received EOF client={client_id} msg_id={msgid}"
        )
        with self.client_acces:
            if client_id not in self.client:
                self.client[client_id] = Connection(client_id)
        self.sum_control_output_exchange.send(message_protocol.internal.serialize([RECV_EOF , client_id, msgid]))
        logging.info(f"Broadcasting EOF message")

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 4:
            self._process_data(*fields)
        elif len(fields) == 2:
            self._process_eof(*fields)
        else:
            nack()
            return
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
