from confluent_kafka import Consumer, KafkaError
import mysql.connector
import csv
from io import StringIO
from datetime import datetime

# Configueer consumer 
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)

# Subscribe to the Kafka topic
consumer.subscribe(['weddenschap_winstkansen'])

# Configureer database connectie
config = {
    'user': 'root',
    'password': 'root',
    'host': 'LAPTOP-1HKCQFQU',
    'database': 'DEP_DWH_G30',
    'raise_on_warnings': True
}

# Connecteer met database
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

        # processeer het bericht
        data = msg.value().decode("utf-8")
        print(f'Received message: {data}')
        
        # Use the csv module to parse the CSV data
        reader = csv.reader(StringIO(data))
        for row in reader:
            id, wedstrijd, starttijd, thuisploeg, uitploeg, vraag, keuze, kans, timestamp_str = row
            
            #Converteer startij juist 
            starttijd = starttijd.replace('T', ' ').replace('Z', '')
            
            # Converteer data timestamp juiste formaat
            timestamp = datetime.strptime(timestamp_str, '%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            
            # Zet data in DWH 
            sql_query = f"""
            INSERT INTO weddenschap_winstkansen (ID, Wedstrijd, Starttijd, Thuisploeg, Uitploeg, Vraag, Keuze, Kans, Timestamp)
            VALUES ('{id}', '{wedstrijd}', '{starttijd}', '{thuisploeg}', '{uitploeg}', '{vraag}', '{keuze}', {kans}, '{timestamp}');
            """
            db_cursor.execute(sql_query)
            db_conn.commit()

except KeyboardInterrupt:
    pass

finally:
    # Sluit Consumer
    consumer.close()
    # Sluit database connectie
    db_cursor.close()
    db_conn.close()
