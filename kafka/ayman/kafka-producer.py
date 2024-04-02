import csv
from confluent_kafka import Producer

def configureer_producer():
    conf = {'bootstrap.servers': 'localhost:9092'}
    return Producer(conf)

def leveringsrapport(err, msg):
    if err is not None:
        print(f'Fout bij het verzenden van bericht: {err}')
    else:
        print(f'Bericht succesvol verzonden naar {msg.topic()}')

def verwerk_csv_bestand(producer, csv_file_path):
    with open(csv_file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            # Converteer de rij naar een string (of een ander geschikt formaat)
            row_str = ','.join(row)
            # Verzend de rij naar het Kafka-topic
            producer.produce('weddenschap_winstkansen', key='key', value=row_str, callback=leveringsrapport)

def hoofdfunctie():
    producer = configureer_producer()
    csv_file_path = 'bets.csv'
    verwerk_csv_bestand(producer, csv_file_path)
    # Wacht op alle verzonden berichten te worden bevestigd
    producer.flush()

if __name__ == "__main__":
    hoofdfunctie()
