"""
Este módulo se encarga de escuchar cambios en la base de datos y enviar mensajes a RabbitMQ
cuando se detectan nuevas Preguntas
"""

import time
import pika
import pyodbc

def get_db_connection():
    """
    Establece y devuelve una conexión a la base de datos en Azure.
    """
    connection_string = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=tcp:preguntasrec.database.windows.net,1433;"
        "Database=PreguntasyRespuestas;"
        "Uid=admin2024;"
        "Pwd=AdminR2024;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    conn = pyodbc.connect(connection_string)
    return conn


def listen_to_db_changes():
    """
    Escucha cambios en la base de datos y envía mensajes a RabbitMQ si se detectan nuevos preguntas.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("Conexión establecida con la base de datos.")

    preguntas_table = 'Preguntas'
    log_table = 'ChangeLog'

    cursor.execute(f"SELECT MAX(IdPregunta) FROM {log_table}")
    last_logged_id = cursor.fetchone()[0]

    if last_logged_id is None:
        last_logged_id = 0

    print(f"Último ID registrado en el log: {last_logged_id}")

    while True:
        cursor.execute(
            f"SELECT PreguntaID, TextoPregunta FROM {preguntas_table} WHERE PreguntaID > ?", 
            last_logged_id
        )
        new_pregunta = cursor.fetchall()

        if new_pregunta:
            connection = pika.BlockingConnection(pika.ConnectionParameters('host.docker.internal'))
            channel = connection.channel()
            channel.queue_declare(queue='pregunta_updates')

            for pregunta in new_pregunta:
                pregunta_id, pregunta_text = pregunta
                message = (
                    f"Se han añadido nuevas preguntas deberías ir a hacerlas: (ID: {pregunta_id})"
                )
                channel.basic_publish(exchange='', routing_key='pregunta_updates', body=message)
                print(f" [x] Sent '{message}'")

                cursor.execute(f"SELECT COUNT(*) FROM {log_table} WHERE IdPregunta = ?", pregunta_id)
                exists = cursor.fetchone()[0]
                
                if exists == 0:
                    cursor.execute(f"INSERT INTO {log_table} (IdPregunta) VALUES (?)", pregunta_id)
                    conn.commit()
                    print(f" [x] Added IdPregunta {pregunta_id} to ChangeLog")

            connection.close()

            last_logged_id = max(product[0] for product in new_pregunta)

        time.sleep(10)

if __name__ == '__main__':
    listen_to_db_changes()
