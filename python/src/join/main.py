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


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        self.output_queue.send(message)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

def handle_sigterm(input_queue):
    try:
        input_queue.stop_consuming()
    except middleware.MessageMiddlewareDisconnectedError as e:
        logging.error(f"Error stopping consumer: {e}")
    except middleware.MessageMiddlewareMessageError as e:
        logging.error(f"Error stopping consumer: {e}")

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
