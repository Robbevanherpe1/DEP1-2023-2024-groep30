# Linux VM Commands

## De basisconfiguratie

- ssh -p 40197 vicuser@vichogent.be

## Microsoft SQL Server

### Install SQL Server
- sudo curl -o /etc/yum.repos.d/mssql-server.repo https://packages.microsoft.com/config/rhel/9/mssql-server-2022.repo
- sudo yum install -y mssql-server
- sudo /opt/mssql/bin/mssql-conf setup (pswd = VMdepgroup30)
- systemctl status mssql-server
- sudo firewall-cmd --zone=public --add-port=1433/tcp --permanent
- sudo firewall-cmd --reload

### Install the SQL Server command-line tools
- curl https://packages.microsoft.com/config/rhel/9/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo (Red Hat 9)
- sudo - yum remove mssql-tools unixODBC-utf16 unixODBC-utf16-devel
- sudo yum install -y mssql-tools18 unixODBC-develyze
- sudo yum check-update sudo yum update mssql-tools18
- echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bash_profile
- echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc source ~/.bashrc

### Connect locally
- sqlcmd -S localhost -U sa -P 'VMdepgroup30' -C

### Create and query data
- CREATE DATABASE TestDB;
- SELECT Name from sys.databases;
- GO

- USE TestDB;
- CREATE TABLE dbo.Inventory ( id INT, name NVARCHAR(50), quantity INT, PRIMARY KEY (id) );
- INSERT INTO dbo.Inventory VALUES (1, 'banana', 150);
- INSERT INTO dbo.Inventory VALUES (2, 'orange', 154);
- GO

- SELECT * FROM dbo.Inventory WHERE quantity > 152;
- GO

- QUIT

## Connecteren met SQL Server

- ssh -L 1500:localhost:1433 10.11.11.30
- ssh -vv -L 1500:localhost:1433 vichogent.be -o ConnectTimeout=100 -p 40197
- sqlcmd -H "127.0.0.1,1500" -U sa -P VMdepgroup30

## Het schedulen van de scripts

### De weddenschappen
- nano fetch_bets.py
- chmod +x fetch_bets.py

### De wedstrijduitslagen
- nano fetch_wedstrijden.py
- chmod +x fetch_wedstrijden.py

### Crontab
- rpm -q cronie
- sudo yum install cronie
- sudo systemctl status crond.service

- nano c.txt
- crontab < c.txt
- crontab -l

0 0 * * * /usr/bin/python3 /home/vicuser/data_fetch/fetch_bets.py
0 0 * * * /usr/bin/python3 /home/vicuser/data_fetch/fetch_wedstrijden.py

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
3. Topics Aanmaken
## 3. Topics Aanmaken

Nu Kafka en ZooKeeper draaien, kun je topics aanmaken.

1. **Open een nieuw Command Prompt of PowerShell-venster**: Dit is nodig omdat we de Kafka broker en ZooKeeper in de achtergrond laten draaien.

2. **Maak de topics aan**: Gebruik het volgende commando om de twee gevraagde topics aan te maken:
powershell .\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic weddenschap_winstkansen .\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic wedstrijduitslag

4. Controleren van Berichten in een Topic
## 4. Controleren van Berichten in een Topic

Om te controleren hoeveel berichten er momenteel in een topic staan, kun je het `kafka-console-consumer` commando gebruiken. Dit commando leest berichten van een topic en toont ze in de terminal.

Voorbeeld om berichten van de `weddenschap_winstkansen` topic te lezen:

powershell .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic weddenschap_winstkansen --from-beginning


De optie `--from-beginning` zorgt ervoor dat alle berichten in de topic worden gelezen, vanaf het begin. Als je alleen de nieuwste berichten wilt lezen, kun je deze optie weglaten.
Let op
## Let op

- Zorg ervoor dat je de juiste versies van Java en Apache Kafka gebruikt die compatibel zijn met elkaar.
- De commando's kunnen variëren afhankelijk van je specifieke systeemconfiguratie en de versies van de software die je gebruikt.
- Voor productieomgevingen is het aanbevolen om Kafka en ZooKeeper te configureren voor hoge beschikbaarheid en veiligheid.