import pika
import sys
import signal
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

# URL de conexão com o RabbitMQ
RABBITMQ_URL = os.getenv('RABBITMQ_URL')
if RABBITMQ_URL is None:
    print('RABBITMQ_URL not found in .env')
    sys.exit(1)

# Callback é chamado quando uma mensagem é recebida e printa a mensagem


def callback(ch, method, properties, body):
    print(f" [x] Received: {body}")

# Fecha a conexão com o RabbitMQ e encerra o programa


def close_connection():
    print(" [x] Closing connection")
    channel.stop_consuming()
    connection.close()
    print(" [x] Connection closed")
    sys.exit(0)

# Função chamada para enviar uma mensagem para uma fila


def send_message(queue, message):
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='', routing_key=queue, body=message)
    print(f" [x] Sent '{message}'")


# Captura o sinal de interrupção (CTRL+C) e chama a função close_connection
signal.signal(signal.SIGINT, lambda sig, frame: close_connection())

# Cria uma conexão com o RabbitMQ
params = pika.URLParameters(RABBITMQ_URL)  # substitua pela sua URL

connection = pika.BlockingConnection(params)

# Cria um canal e declara uma fila
channel = connection.channel()  # cria um canal

# Enviar requisição para a tarefa 1
send_message('reply-message', 'Hello from Python!')

# Enviar requisição para a tarefa 2
send_message('write_in_file', 'Write this in a file')

# Enviar requisição para a tarefa 3
send_message('sum', '50, 30')

# Espera por mensagens, e chama o callback quando uma mensagem é recebida
channel.basic_consume(
    queue='hello-resp', on_message_callback=callback, auto_ack=True)

# Inicia o consumo de mensagens
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
