import os
import logging
import threading

from common import middleware, message_protocol, fruit_item
from python.src.common.fruit_item.fruit_item import FruitItem

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
#Estos serian los 2 tipos de mensajes enviados por el exchange de control
RECV_EOF = 1
REPORT_MSG  = 2


def fill_set(eof_msg_id)-> set:
    res = set()
    i = 1
    while i < SUM_AMOUNT and (eof_msg_id-i) >= 0:
        res.add(eof_msg_id-i)
        i += 1
    return res

class Connection:
    def __init__(self, id):
        self.fruits ={}
        self.id = id
        self.last_msg = -1
        self.msg_set = set()

    def add_fruit(self, fruit, amount):
        fruit_item = FruitItem(fruit, int(amount))
        if fruit not in self.fruits:
            self.fruits[fruit] = fruit_item
        else:
            self.fruits[fruit] = self.fruits[fruit] + fruit_item 

class SumFilter:
        
    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 3:
            logging.error(f"Invalid control message received: {fields}")
            nack()
            return
        [msg_type, client_id, msg_id] = fields
        if msg_type == RECV_EOF:
            logging.info(f"Received EOF message from client {client_id} with msg_id {msg_id}")
            with self.client_acces:
                conn = self.client[client_id]
                conn.msg_set = fill_set(msg_id)
                if conn.last_msg in conn.msg_set:
                    self.client[client_id].msg_set.remove(conn.last_msg)
                    self.sum_control_exchange.send(message_protocol.internal.serialize([REPORT_MSG , client_id, conn.last_msg]))
                
        elif msg_type == REPORT_MSG :
            logging.info(f"Received DATA message from client {client_id} with msg_id {msg_id}")
            madeit = False
            with self.client_acces:
                conn = self.client[client_id]
                if msg_id in conn.msg_set:
                    conn.msg_set.remove(msg_id)
                    if len(conn.msg_set) == 0:
                        madeit = True
            if madeit:
                #enviar resultados a aggregate
                pass
        else:
            logging.error(f"Invalid control message type received: {msg_type}")
            nack()
            return 
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
        self.control_thread = threading.Thread(self.handle_control_exchange)
        self.control_thread.start()

    def _process_data(self, id, msgid, fruit, amount):
        logging.info(f"Process data")
        madeit = False
        report = False
        with self.client_acces:
            if id not in self.client:
                self.client[id] = Connection(id)
            conn = self.client[id]
            conn.add_fruit(fruit,amount)
            conn.last_msg = msgid
            conn = self.client[id]
            if msgid in conn.msg_set:
                conn.msg_set.remove(msgid)
                report = True
                if len(conn.msg_set) == 0:
                    madeit = True
        if report:
            self.sum_control_exchange.send(message_protocol.internal.serialize([REPORT_MSG , id, conn.last_msg]))

        if madeit:
            #enviar a aggregators
            pass


            
        

    def _process_eof(self, client_id, msgid):
        # logging.info(f"Broadcasting data messages")
        # for final_fruit_item in self.amount_by_client_fruit.values():
        #     for data_output_exchange in self.data_output_exchanges:
        #         data_output_exchange.send(
        #             message_protocol.internal.serialize(
        #                 [final_fruit_item.fruit, final_fruit_item.amount]
        #             )
        #         )

        # logging.info(f"Broadcasting EOF message")
        # for data_output_exchange in self.data_output_exchanges:
        #     data_output_exchange.send(message_protocol.internal.serialize([]))
        logging.info(f"Broadcasting data messages")
        with self.client_acces:
            conn = self.client[client_id]
            conn.msg_set = fill_set(msgid)
        self.sum_control_exchange.send(message_protocol.internal.serialize([RECV_EOF , client_id, msgid]))
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
