import uuid
from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.msg_id = -1
        pass
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.msg_id += 1
        return message_protocol.internal.serialize([self.id, self.msg_id, fruit, amount])

    def serialize_eof_message(self, message):
        self.msg_id += 1
        return message_protocol.internal.serialize([self.id, self.msg_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        #TPDO para ver con el protocolo
        return fields
