from confluent_kafka import Consumer, KafkaError
import mysql.connector
import csv
from io import StringIO
from datetime import datetime

def configureer_consumer():
    conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'auto.offset.reset': 'earliest'}
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
        # Controleer of de eerste element 'ID' is, wat aangeeft dat we de kolomkoppen hebben bereikt
        if row[0] == 'ID':
            continue
        
        id, wedstrijd, starttijd, thuisploeg, uitploeg, vraag, keuze, kans, *timestamp_str = row
        
        # Formeer de starttijd correct
        starttijd = starttijd.replace('T', ' ').replace('Z', '')
        
        # Als timestamp_str niet leeg is, parse het; anders gebruik de huidige tijd
        if timestamp_str:
            if timestamp_str[0]:
                timestamp = datetime.strptime(timestamp_str[0], '%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            else:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Voeg gegevens toe aan DWH
        sql_query = f"""
        INSERT INTO weddenschap_winstkansen (ID, Wedstrijd, Starttijd, Thuisploeg, Uitploeg, Vraag, Keuze, Kans, Timestamp)
        VALUES ('{id}', '{wedstrijd}', '{starttijd}', '{thuisploeg}', '{uitploeg}', '{vraag}', '{keuze}', {kans}, '{timestamp}');
        """
        db_cursor.execute(sql_query)

def hoofdfunctie():
    consumer = configureer_consumer()
    db_conn = configureer_database_verbinding()
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

            verwerk_bericht(msg, db_cursor)
            db_conn.commit()

    except KeyboardInterrupt:
        pass

    finally:
        # Sluit Consumer
        consumer.close()
        # Sluit database verbinding
        db_cursor.close()
        db_conn.close()

if __name__ == "__main__":
    hoofdfunctie()
