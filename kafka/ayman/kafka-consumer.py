from confluent_kafka import Consumer, KafkaError
import psycopg2
import mysql.connector


# Configureer de consumer
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)

# Abonneer op het Kafka topic
consumer.subscribe(['weddenschap_winstkansen'])

# Configureer de databaseverbinding
config = {
 'user': 'root',
 'password': 'root',
 'host': 'LAPTOP-1HKCQFQU',
 'database': 'DEP_DWH_G30',
 'raise_on_warnings': True
}

# Maak een verbinding met de database
db_conn = mysql.connector.connect(**config)

db_cursor = db_conn.cursor()

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Verwerk het bericht en voer de SQL-query uit
        data = msg.value().decode("utf-8")
        print(f'Ontvangen bericht: {data}')
        
        sql_query = f"INSERT INTO weddenschap_winstkansen (kolom) VALUES ('{data}');"
        db_cursor.execute(sql_query)
        db_conn.commit()

except KeyboardInterrupt:
    pass

finally:
    # Verwijder de consumer
    consumer.close()
    # Sluit de databaseverbinding
    db_cursor.close()
    db_conn.close()