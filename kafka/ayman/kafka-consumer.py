from confluent_kafka import Consumer, KafkaError
import mysql.connector
import csv
from io import StringIO
from datetime import datetime

# Configure consumer 
conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)

# Subscribe to the Kafka topic
consumer.subscribe(['weddenschap_winstkansen'])

# Configure database connection
config = {
    'user': 'root',
    'password': 'root',
    'host': 'LAPTOP-1HKCQFQU',
    'database': 'DEP_DWH_G30',
    'raise_on_warnings': True
}

# Connect to the database
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

        # Process the message
        data = msg.value().decode("utf-8")
        print(f'Received message: {data}')
        
        # Use the csv module to parse the CSV data
        reader = csv.reader(StringIO(data))
        for row in reader:
            id, wedstrijd, starttijd, thuisploeg, uitploeg, vraag, keuze, kans, *timestamp_str = row
            
            # Correctly format start time
            starttijd = starttijd.replace('T', ' ').replace('Z', '')
            
            # If timestamp_str is not empty, parse it; otherwise, use the current timestamp
            if timestamp_str:
                timestamp = datetime.strptime(timestamp_str[0], '%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
            else:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Use current timestamp as default
            
            # Insert data into DWH 
            sql_query = f"""
            INSERT INTO weddenschap_winstkansen (ID, Wedstrijd, Starttijd, Thuisploeg, Uitploeg, Vraag, Keuze, Kans, Timestamp)
            VALUES ('{id}', '{wedstrijd}', '{starttijd}', '{thuisploeg}', '{uitploeg}', '{vraag}', '{keuze}', {kans}, '{timestamp}');
            """
            db_cursor.execute(sql_query)
            db_conn.commit()

except KeyboardInterrupt:
    pass

finally:
    # Close Consumer
    consumer.close()
    # Close database connection
    db_cursor.close()
    db_conn.close()
