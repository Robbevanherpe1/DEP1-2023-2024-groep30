from confluent_kafka import Consumer, KafkaError
import mysql.connector
import csv
from io import StringIO
from datetime import datetime

def configureer_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9092', 
        'group.id': 'mygroup', 
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['weddenschap_winstkansen'])
    return consumer

def configureer_database_verbinding():
    config = {
        'user': 'root',
        'password': 'root',
        'host': 'LAPTOP-1HKCQFQU',
        'database': 'DEP_DWH_G30',
        'raise_on_warnings': True
    }
    return mysql.connector.connect(**config)

def verwerk_bericht(msg, db_cursor):
    data = msg.value().decode("utf-8")
    print(f'Bericht ontvangen: {data}')
    
    reader = csv.reader(StringIO(data))
    for row in reader:
        if row[0] == 'ID':  # Sla de kolomkoppen over
            continue
        
        id, wedstrijd, starttijd, thuisploeg, uitploeg, vraag, keuze, kans, *timestamp_str = row
        starttijd = starttijd.replace('T', ' ').replace('Z', '')
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') if not timestamp_str else timestamp_str[0]
        
        query = """
        INSERT INTO weddenschap_winstkansen (ID, Wedstrijd, Starttijd, Thuisploeg, Uitploeg, Vraag, Keuze, Kans, Timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        db_cursor.execute(query, (id, wedstrijd, starttijd, thuisploeg, uitploeg, vraag, keuze, kans, timestamp))

def hoofdfunctie():
    consumer = configureer_consumer()
    with configureer_database_verbinding() as db_conn:
        with db_conn.cursor() as db_cursor:
            try:
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None or msg.error() and msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    elif msg.error():
                        print(msg.error())
                        break

                    verwerk_bericht(msg, db_cursor)
                    db_conn.commit()

            except KeyboardInterrupt:
                pass
            finally:
                consumer.close()

if __name__ == "__main__":
    hoofdfunctie()