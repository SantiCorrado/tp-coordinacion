import os
import logging
import signal
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

class Connection:
    def __init__(self, id):
        self.fruits ={}
        self.id = id
        self.sum_set = set()
        self.result_sent = False

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.clients = {}

    def _process_data(self, client_id, sum_id, item):
        logging.info(
            f"Aggregation {ID}: received "
            f"client={client_id}, sum={sum_id}"
        )
        if client_id not in self.clients:
            self.clients[client_id] = Connection(client_id)
        conn = self.clients[client_id]
        if item.fruit not in conn.fruits:
            conn.fruits[item.fruit] = item
        else:
            conn.fruits[item.fruit] += item

    def _process_eof(self,client_id, sum_id):
        logging.info(
            f"Aggregation {ID}: received "
            f"client={client_id}, sum={sum_id}"
        )
        if client_id not in self.clients:
            self.clients[client_id] = Connection(client_id)
        conn = self.clients[client_id]
        conn.sum_set.add(sum_id)
        logging.info(
            f"Aggregation EOF {ID}: client={client_id}, "
            f"sum_set={conn.sum_set}"
        )
        if len(conn.sum_set) == SUM_AMOUNT and not conn.result_sent:
            conn.result_sent = True
            for r in list(conn.fruits.values()):
                self.output_queue.send(message_protocol.internal.serialize(message_protocol.internal.DATA, client_id, ID, r))
            self.output_queue.send(message_protocol.internal.serialize(message_protocol.internal.EOF, client_id, "", ID))


    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
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
        self.input_exchange.start_consuming(self.process_messsage)

def handle_sigterm(input_exchange):
    try:
        input_exchange.stop_consuming()
    except middleware.MessageMiddlewareDisconnectedError as e:
        logging.error(f"Error stopping consumer: {e}")
    except middleware.MessageMiddlewareMessageError as e:
        logging.error(f"Error stopping consumer: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(
        signal.SIGTERM,
        lambda signum, frame: handle_sigterm(aggregation_filter.input_exchange),)
    
    aggregation_filter.start()
    try:
        aggregation_filter.input_exchange.close()
    except middleware.MessageMiddlewareCloseError as e:
        logging.error(f"Error closing input exchange: {e}")
    try:
        aggregation_filter.output_queue.close()
    except middleware.MessageMiddlewareCloseError as e:
        logging.error(f"Error closing input exchange: {e}")
    logging.info("Aggregation stopped")
    return 0


if __name__ == "__main__":
    main()
