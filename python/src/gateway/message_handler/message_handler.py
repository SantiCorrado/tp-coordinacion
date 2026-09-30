import logging
import uuid
from common import message_protocol, fruit_item

class MessageHandler:

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.msg_id = -1
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.msg_id += 1
        return message_protocol.internal.serialize(message_protocol.internal.DATA,self.id, self.msg_id, fruit_item.FruitItem(fruit, amount))

    def serialize_eof_message(self, message):
        self.msg_id += 1
        return message_protocol.internal.serialize(message_protocol.internal.EOF,self.id, "", self.msg_id)

    def deserialize_result_message(self, message):
        logging.info(
            f"Gateway: received result message={message_protocol.internal.deserialize(message)}"
        )
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 2:
            return None
        [client_id, top_item] = fields
        if client_id != self.id:
            return None
        logging.info(
            f"sending result message={message_protocol.internal.deserialize(message)}"
        )
        top = []
        for ti in top_item:
            top.append([ti.fruit, ti.amount])
        return top

