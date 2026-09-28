import os
import logging
import bisect

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


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.clients = {}

    def _process_data(self, client_id, sum_id, sum_result):
        logging.info(
            f"Aggregation {ID}: received "
            f"client={client_id}, sum={sum_id}"
        )
        if client_id not in self.clients:
            self.clients[client_id] = Connection(client_id)
        conn = self.clients[client_id]
        for (fruit, amount) in sum_result:
            item = fruit_item.FruitItem(fruit,amount)
            if fruit not in conn.fruits:
                conn.fruits[fruit] = item
            else:
                conn.fruits[fruit] += item
        conn.sum_set.add(sum_id)
        logging.info(
            f"Aggregation {ID}: client={client_id}, "
            f"sum_set={conn.sum_set}"
        )
        if len(conn.sum_set) == SUM_AMOUNT:
            top_item = sorted(conn.fruits.values(),reverse=True)[:TOP_SIZE]
            top = []
            for ti in top_item:
                top.append([ti.fruit, ti.amount])
            logging.info(
                f"Aggregation {ID}: COMPLETE client={client_id}, "
                f"top={top}"
            )
            self.output_queue.send(message_protocol.internal.serialize([client_id, top]))


    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            nack()
            return
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
