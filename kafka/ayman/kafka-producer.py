import csv
from confluent_kafka import Producer

# Configureer de producer
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Fout bij het verzenden van bericht: {err}')
    else:
        print(f'Bericht succesvol verzonden naar {msg.topic()}')

# Specificeer het pad naar je CSV-bestand
csv_file_path = 'bets.csv'

# Open het CSV-bestand
with open(csv_file_path, 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        # Converteer de rij naar een string (of een andere geschikte formaat)
        row_str = ','.join(row)
        # Verzend de rij naar het Kafka-topic
        producer.produce('weddenschap_winstkansen', key='key', value=row_str, callback=delivery_report)

# Wacht op alle verzonden berichten te worden bevestigd
producer.flush()