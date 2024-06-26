#### Kafka 


## 1. Java Installeren

1. **Download de Java LTS-versie**: Ga naar de [officiële Oracle Java downloadpagina](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html) en download de LTS-versie voor je systeem.

2. **Installeer Java**: Volg de instructies op de downloadpagina om Java te installeren.

3. **Controleer de installatie**: Open een Command Prompt en voer het volgende commando uit om te controleren of Java correct is geïnstalleerd:
powershell java -version

Je zou de versie van de geïnstalleerde Java moeten zien.
2. Apache Kafka Installeren

## 2. Apache Kafka Installeren

1. **Download Apache Kafka**: Ga naar de [Apache Kafka downloadpagina](https://kafka.apache.org/downloads) en download de nieuwste versie.

2. **Unzip Kafka**: Unzip het gedownloade bestand naar een gewenste locatie.

3. **Start ZooKeeper**: Apache Kafka gebruikt ZooKeeper voor clustercoördinatie. Start ZooKeeper met het volgende commando:
powershell .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Laat dit commando draaien in een venster.

4. **Start Kafka Broker**: Open een nieuw Command Prompt of PowerShell-venster en start de Kafka broker met het volgende commando:
powershell .\bin\windows\kafka-server-start.bat .\config\server.properties

Laat dit commando ook draaien in een venster.

## 3. Topics Aanmaken

Nu Kafka en ZooKeeper draaien, kun je topics aanmaken.

1. **Open een nieuw Command Prompt of PowerShell-venster**: Dit is nodig omdat we de Kafka broker en ZooKeeper in de achtergrond laten draaien.

2. **Maak de topics aan**: Gebruik het volgende commando om de twee gevraagde topics aan te maken:
powershell .\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic weddenschap_winstkansen .\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic wedstrijduitslag

## 4. Controleren van Berichten in een Topic

Om te controleren hoeveel berichten er momenteel in een topic staan, kun je het `kafka-console-consumer` commando gebruiken. Dit commando leest berichten van een topic en toont ze in de terminal.

Voorbeeld om berichten van de `weddenschap_winstkansen` topic te lezen:

powershell .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic weddenschap_winstkansen --from-beginning


De optie `--from-beginning` zorgt ervoor dat alle berichten in de topic worden gelezen, vanaf het begin. Als je alleen de nieuwste berichten wilt lezen, kun je deze optie weglaten.
Let op
