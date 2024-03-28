from kafka import KafkaProducer
import pandas as pd
import json

# Kafka-producer initialiseren
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# CSV-data laden
df = pd.read_csv('../bets.csv')

# Kafka-topic
topic_naam = 'WeddenschapWinstKansen'

# Stuur elke rij naar het Kafka-topic
for index, row in df.iterrows():
    bericht = row.to_json()
    producer.send(topic_naam, value=bericht)
    producer.flush()

print("Alle berichten zijn naar Kafka-topic gestuurd.")
