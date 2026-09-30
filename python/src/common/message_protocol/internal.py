from common import fruit_item
RECV_EOF = "1" #indica que uno de los sum obtuvo el eof de uno de los clientes
REPORT_MSG  = "2" #indica que uno de los clientes recibio uno de los ultimos mensajes
DATA = "3"
EOF = "4"
TOP = "5"

def fruit_list_string(list):
    acum = ""
    i = 0
    for fitem in list:
        acum += str(fitem)
        if i != (len(list) - 1):
            acum += "/"
        i += 1
    return acum

def string_fruit_item(string):
    acum = []
    if not string:
        return []
    for f in string.split('/'):
        fruit, amount = f.split()
        acum.append(fruit_item.FruitItem(fruit, int(amount)))
    return acum

def serialize(type, id1, id2, payload):
    if type == RECV_EOF or type == REPORT_MSG:
        msg = str(type) + "," + str(id1) + "," + str(id2) + "," + str(payload)
        return msg.encode("utf-8")
    if type == DATA:
        msg = str(type) + "," + str(id1) + "," + str(id2) + "," + fruit_list_string(payload)
        return msg.encode("utf-8")
    elif type == EOF:
        msg = str(type) + "," + str(id1) + "," + str(payload)
        return msg.encode("utf-8")
    elif type == TOP:
        msg = str(type) + "," + str(id1) + "," + fruit_list_string(payload)
        return msg.encode("utf-8")
    else:
        payload = str(payload)


def deserialize(message):
    fields = message.decode("utf-8").split(',')
    if fields[0] == RECV_EOF or fields[0] == REPORT_MSG:
        return [fields[0] , fields[1], int(fields[2]), int(fields[3])]
    if fields[0] == DATA:
        return [fields[1], fields[2], string_fruit_item(fields[3])]
    elif fields[0] == EOF:
        return [fields[1], int(fields[2])]
    elif fields[0] == TOP:
        return [fields[1], string_fruit_item(fields[2])]
    return []
