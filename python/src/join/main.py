import os
import logging
import signal
from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
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
        self.aggregation_set = set()
        self.result_sent = False

class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.clients = {}

    def _process_data(self, client_id, aggregation_id, item):
        logging.info(
            f"join : received "
            f"client={client_id}, aggregation={aggregation_id}"
        )
        if client_id not in self.clients:
            self.clients[client_id] = Connection(client_id)
        conn = self.clients[client_id]
        if item.fruit not in conn.fruits:
            conn.fruits[item.fruit] = item
        else:
            conn.fruits[item.fruit] += item

    def _process_eof(self,client_id, aggregation_id):
        logging.info(
            f"join : received "
            f"client={client_id}, aggregation={aggregation_id}"
        )
        if client_id not in self.clients:
            self.clients[client_id] = Connection(client_id)
        conn = self.clients[client_id]
        conn.aggregation_set.add(aggregation_id)
        logging.info(
            f"join : client={client_id}, "
            f"aggregation_set={conn.aggregation_set}"
        )
        if len(conn.aggregation_set) == AGGREGATION_AMOUNT and not conn.result_sent:
            conn.result_sent = True
            top = sorted(conn.fruits.values(),reverse=True)[:TOP_SIZE]
            logging.info(
                f"join : COMPLETE client={client_id}, "
                f"top={top}"
            )
            self.output_queue.send(message_protocol.internal.serialize(message_protocol.internal.TOP,client_id, "", top))


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
        self.input_queue.start_consuming(self.process_messsage)

def handle_sigterm(input_queue):
    try:
        input_queue.stop_consuming()
    except middleware.MessageMiddlewareDisconnectedError as e:
        logging.error(f"Error stopping join: {e}")
    except middleware.MessageMiddlewareMessageError as e:
        logging.error(f"Error stopping join: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(
            signal.SIGTERM,
            lambda signum, frame: handle_sigterm(join_filter.input_queue),)
    join_filter.start()
    try:
        join_filter.input_queue.close()
    except middleware.MessageMiddlewareCloseError as e:
        logging.error(f"Error closing input exchange: {e}")
    try:
        join_filter.output_queue.close()
    except middleware.MessageMiddlewareCloseError as e:
        logging.error(f"Error closing input exchange: {e}")
    logging.info("join stopped")
    return 0


if __name__ == "__main__":
    main()
