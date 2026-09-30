import os
import logging
import threading
import zlib
import signal
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

def aggregator_responsable(client_id):
    hash = zlib.crc32(client_id.encode("utf-8"))
    return hash % AGGREGATION_AMOUNT

def condition_ready(conn)->bool:
    if len(conn.state) != SUM_AMOUNT or conn.recv_eof is None:
        return False
    acum = 0
    for q in conn.state.values():
        acum += q
    return acum == conn.recv_eof


class Connection:
    def __init__(self, id):
        self.fruits ={}
        self.id = id
        self.msg_ids = set() #ids de mensajes recibidos por el gateway
        self.state = {}
        self.recv_eof = None
        self.result_sent = False

    def add_fruit(self, fruititem):
        if fruititem.fruit not in self.fruits:
            self.fruits[fruititem.fruit] = fruititem
        else:
            self.fruits[fruititem.fruit] = self.fruits[fruititem.fruit] + fruititem 

class SumFilter:
        
    def __init__(self):
        self.client = {}
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

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 4:
            logging.error(f"Invalid control message received: {fields}")
            nack()
            return
        [msg_type, client_id, sum_id, msg] = fields
        result = None
        if msg_type == message_protocol.internal.RECV_EOF:
            report = 0
            with self.client_acces:
                if client_id not in self.client:
                    self.client[client_id] = Connection(client_id)
                conn = self.client[client_id]
                conn.recv_eof =  msg
                conn.state[ID] = len(conn.msg_ids)
                report = len(conn.msg_ids)
                if condition_ready(conn) and not conn.result_sent:
                    conn.result_sent = True
                    result = list(conn.fruits.values())
            self.sum_control_exchange.send(message_protocol.internal.serialize(message_protocol.internal.REPORT_MSG , client_id, ID, report))             
        elif msg_type == message_protocol.internal.REPORT_MSG :
            with self.client_acces:
                if client_id not in self.client:
                    self.client[client_id] = Connection(client_id)
                conn = self.client[client_id]
                conn.state[sum_id] = msg
                if condition_ready(conn) and not conn.result_sent:
                    conn.result_sent = True
                    result = list(conn.fruits.values())
        else:
            logging.error(f"Invalid control message type received: {msg_type}")
            nack()
            return 
        if result is not None:
            self.data_output_exchanges[aggregator_responsable(client_id)].send(message_protocol.internal.serialize(message_protocol.internal.DATA, client_id, ID, result))
        ack()

    def handle_control_exchange(self):
        logging.info(f"Starting control exchange")
        self.sum_control_exchange.start_consuming(self.process_control_message)

    def _process_data(self, id, msgid, list):
        logging.info(f"Process data")
        logging.info(
            f"Sum {ID}: received client={id} msg_id={msgid}"
        )
        report = None
        with self.client_acces:
            if id not in self.client:
                self.client[id] = Connection(id)
            conn = self.client[id]
            conn.msg_ids.add(msgid)
            for fruitItem in list:
                conn.add_fruit(fruitItem)
            conn = self.client[id]
            if conn.recv_eof is not None:
                report = len(conn.msg_ids)
        if report is not None:
            logging.info(
                f"SUM {ID}: received final msg "
                f"client={id}, msgid={report}"
            )
            self.sum_control_output_exchange.send(message_protocol.internal.serialize(message_protocol.internal.REPORT_MSG , id, ID, report))
        
    def _process_eof(self, client_id, msgid):
        logging.info(f"Broadcasting data messages")
        logging.info(
            f"Sum {ID}: received EOF client={client_id} msg_id={msgid}"
        )
        with self.client_acces:
            if client_id not in self.client:
                self.client[client_id] = Connection(client_id)
        self.sum_control_output_exchange.send(message_protocol.internal.serialize(message_protocol.internal.RECV_EOF , client_id, ID, msgid))
        logging.info(f"Broadcasting EOF message")

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 2:
            self._process_eof(*fields)
        else:
            nack()
            return
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

    def shutdown(self):
        if self.control_thread.is_alive():
            self.control_thread.join()
        try:
            self.input_queue.close()
        except middleware.MessageMiddlewareCloseError as e:
            logging.error(f"Error closing input queue: {e}")
        try:
            self.sum_control_exchange.close()
        except  middleware.MessageMiddlewareCloseError as e:
            logging.error(f"Error closing control consumer {e}")
        for exchange in self.data_output_exchanges:
            try:
                exchange.close()
            except middleware.MessageMiddlewareCloseError as e:
                logging.error("Error closing data output")
        try:
            self.sum_control_output_exchange.close()
        except middleware.MessageMiddlewareCloseError as e:
            logging.error("Error stopping control consumer")
        logging.info("Sum shutdown succesful")

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    def handle_sigterm(signum, frame):
        try:
            sum_filter.input_queue.stop_consuming()
        except middleware.MessageMiddlewareDisconnectedError as e:
            logging.error(f"Error stopping input queue: {e}")
        except middleware.MessageMiddlewareMessageError  as e:
            logging.error(f"Error stopping input queue: {e}")
        try:
            sum_filter.sum_control_exchange.stop_consuming()
        except middleware.MessageMiddlewareDisconnectedError as e:
            logging.error(f"Error stopping input queue: {e}")
        except middleware.MessageMiddlewareMessageError  as e:
            logging.error(f"Error stopping input queue: {e}")

    signal.signal(signal.SIGTERM, handle_sigterm)
    sum_filter.start()
    sum_filter.shutdown()
    return 0


if __name__ == "__main__":
    main()
