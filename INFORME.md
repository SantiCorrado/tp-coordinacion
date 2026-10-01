# Protocolo de mensajes interno

El formato de los mensajes depende según el tipo de mensaje a enviar y los distintos tipos son:

- RECV_EOF: mensaje de control entre instancias de sum que informa el id del mensaje EOF de un mensaje.
- REPORT_MSG: mensaje de control entre instancias de sum que informa el número de mensajes recibidos de un cliente.
- DATA: mensaje que contiene id de cliente,segundo id (de instancia emisora o msgid como en gateway) y un par [fruit,amount] separados por “ “.
- EOF: indica que la instancia terminó de mandar los datos, contiene id de cliente y un segundo id como payload(de instancia emisora o msgid como en gateway).
- TOP: contiene id de cliente y un listado de pares (fruit,amount) separados por “ ” con cada par separado por “/”.


# Funcionamiento del sistema

## SUM :
Las diferentes instancias de sum reciben la información de compras DATA de diferentes clientes por la misma cola. Cuando un sum recibe el EOF de un cliente, esté publica en un exchange de control entre sum un mensaje de tipo RECV_EOF que contiene id de cliente con el msg id asignado, con esta información se puede saber la cantidad de mensajes enviados por el cliente, con este dato, las demás instancias de sum publican en el exchange de control la cantidad de mensajes recibidos por ese cliente REPORT_MSG y si la suma de todos da el msgid de EOF se envían los resultados de las sumas parciales a los aggregator con mensajes del tipo DATA.

## AGGREGATOR:
El envío de los datos desde las instancias de sum a los aggregator se hace utilizando el resultado del hashing de la combinación de frutas + client_id para decidir el aggregator al que se lo enviará, de esta manera se obtiene la suma total de cada fruta del cliente en las distintas instancias de aggregator, una vez que el sum termina de enviar las sumas parciales, se envía un mensaje tipo EOF a cada instancia de aggregator indicando que este sum finalizó el envío de datos de un cliente particular, una vez que un aggregator reciba un EOF de cada sum para un cliente en particular le envía el resultado de la suma obtenida a él join.

## JOIN:
La instancia de join va a recibir la suma total de cada fruta para cada cliente y al recibir el mensaje EOF de cada aggregatorse arma el top total del cliente y envía un mensaje TOP al gateway.

# Escalabilidad

 El sistema escala respecto clientes y volumen de datos al distribuir el procesamiento de los datos de cada cliente entre los diferentes controladores sin repetir cálculos, las instancias de sum realizan la suma parcial de los registro de compra que reciben, los aggregators obtienen la cantidad total de un conjunto de frutas comprada por un cliente particular (client_id), y el join finalmente une esos valores totales para encontrar al top final.

Si se agregan más instancias de sum al sistema, la distribución de la carga del procesamiento sería mayor, con la consecuencia de mayor cantidad de mensajes de coordinación entre las diferentes instancias de sum.

Al agregar más instancias de aggregator, las combinaciones de (client_id, fruta) se distribuyen entre una mayor cantidad de nodos, repartiendo la carga de procesamiento de las sumas parciales.
