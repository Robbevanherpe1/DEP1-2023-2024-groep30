# Linux VM Commands

## De basisconfiguratie

- ssh -p 40197 vicuser@vichogent.be

- sudo dnf install python3
- sudo dnf install python3-pip
- pip3 install pandas
- pip3 --version
- sudo dnf install unixODBC unixODBC-devel
- pip3 install pyodbc
- pip3 install tqdm
- pip install beautifulsoup4

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

### Create DWH
CREATE DATABASE DEP_DWH_G30;
GO

USE DEP_DWH_G30;
GO

DROP TABLE IF EXISTS factwedstrijdscore, factweddenschap, factklassement;
DROP TABLE IF EXISTS dimkans, dimteam, dimtime, dimwedstrijd;
DROP TABLE IF EXISTS dimdate;
GO

CREATE TABLE DimKans(KansKey INT PRIMARY KEY IDENTITY(1,1),OddsWaarde DECIMAL(5,2) NOT NULL);
GO

CREATE TABLE DimTeam(TeamKey INT PRIMARY KEY IDENTITY(1,1),Stamnummer INT NOT NULL,PloegNaam NVARCHAR(50) NOT NULL);
GO

CREATE TABLE DimDate(DateKey INT PRIMARY KEY IDENTITY(1,1),VolledigeDatumAlternatieveSleutel NVARCHAR(50) NOT NULL,
Datum DATE NOT NULL,DagVanDeMaand INT NOT NULL,DagVanHetJaar INT NOT NULL,WeekVanHetJaar INT NOT NULL,
DagVanDeWeekInMaand INT NOT NULL,DagVanDeWeekInJaar INT NOT NULL,Maand INT NOT NULL,Kwartaal INT NOT NULL,
Jaar INT NOT NULL,EngelseDag NVARCHAR(50) NOT NULL,EngelseMaand NVARCHAR(50) NOT NULL,EngelsJaar NVARCHAR(50) NOT NULL,
DDMMJJJJ NVARCHAR(50) NOT NULL);
GO

CREATE TABLE DimTime(TimeKey INT PRIMARY KEY IDENTITY(1,1),Uur INT NOT NULL,Minuten INT NOT NULL,VolledigeTijd TIME NOT NULL);
GO

CREATE TABLE DimWedstrijd(WedstrijdKey INT PRIMARY KEY IDENTITY(1,1),MatchID INT NOT NULL);
GO

CREATE TABLE FactWedstrijdScore(ScoreID INT PRIMARY KEY IDENTITY(1,1),TeamKeyUit INT NOT NULL,TeamKeyThuis INT NOT NULL,
WedstrijdKey INT NOT NULL,DateKey INT NOT NULL,TimeKey INT NOT NULL,ScoreThuis INT NOT NULL,ScoreUit INT NOT NULL,
EindscoreThuis INT NOT NULL,EindscoreUit INT NOT NULL,ScorendePloegKey INT NOT NULL,FOREIGN KEY (TeamKeyUit) REFERENCES DimTeam(TeamKey),
FOREIGN KEY (TeamKeyThuis) REFERENCES DimTeam(TeamKey),FOREIGN KEY (WedstrijdKey) REFERENCES DimWedstrijd(WedstrijdKey),
FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey));
GO

CREATE TABLE FactWeddenschap(WeddenschapID INT PRIMARY KEY IDENTITY(1,1),TeamKeyUit INT NOT NULL,TeamKeyThuis INT NOT NULL,
WedstrijdKey INT NOT NULL,KansKey INT NOT NULL,DateKeyScrape INT NOT NULL,TimeKeyScrape INT NOT NULL,DateKeySpeeldatum INT NOT NULL,
TimeKeySpeeldatum INT NOT NULL,OddsThuisWint DECIMAL(5,2),OddsUitWint DECIMAL(5,2),OddsGelijk DECIMAL(5,2),OddsBeideTeamsScoren DECIMAL(5,2),
OddsNietBeideTeamsScoren DECIMAL(5,2),OddsMeerDanXGoals DECIMAL(5,2),OddsMinderDanXGoals DECIMAL(5,2),FOREIGN KEY (TeamKeyUit) REFERENCES DimTeam(TeamKey),FOREIGN KEY (TeamKeyThuis) REFERENCES DimTeam(TeamKey),FOREIGN KEY (WedstrijdKey) REFERENCES DimWedstrijd(WedstrijdKey),FOREIGN KEY (KansKey) REFERENCES DimKans(KansKey),FOREIGN KEY (DateKeyScrape) REFERENCES DimDate(DateKey),
FOREIGN KEY (TimeKeyScrape) REFERENCES DimTime(TimeKey),FOREIGN KEY (DateKeySpeeldatum) REFERENCES DimDate(DateKey),
FOREIGN KEY (TimeKeySpeeldatum) REFERENCES DimTime(TimeKey));
GO

CREATE TABLE FactKlassement(KlassementKey INT PRIMARY KEY IDENTITY(1,1),BeginDateKey INT NOT NULL,EindeDateKey INT NOT NULL,
TeamKey INT NOT NULL,Stand INT NOT NULL,AantalGespeeld INT NOT NULL,AantalGewonnen INT NOT NULL,AantalGelijk INT NOT NULL,
AantalVerloren INT NOT NULL,DoelpuntenVoor INT NOT NULL,DoelpuntenTegen INT NOT NULL,DoelpuntenSaldo INT NOT NULL,PuntenVoor INT NOT NULL,
PuntenTegen INT NOT NULL,FOREIGN KEY (BeginDateKey) REFERENCES DimDate(DateKey),
FOREIGN KEY (EindeDateKey) REFERENCES DimDate(DateKey),FOREIGN KEY (TeamKey) REFERENCES DimTeam(TeamKey));
GO

SELECT * FROM DimKans
SELECT * FROM DimTeam
SELECT * FROM DimDate
SELECT * FROM DimWedstrijd
SELECT * FROM FactWedstrijdScore
SELECT * FROM FactWeddenschap
SELECT * FROM FactKlassement
GO

## Connecteren met SQL Server

- ssh -L 1500:localhost:1433 10.11.11.30
- ssh -vv -L 1500:localhost:1433 vichogent.be -o ConnectTimeout=100 -p 40197

## Fill DWH
- nano fillDWH.py
- chmod +x fillDWH.py

## Het schedulen van de scripts

### De weddenschappen
- nano scratch_bets.py
- chmod +x scratch_bets.py

- nano clean_bets.py
- chmod +x clean_bets.py

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

0 8,14,20 * * * /usr/bin/python3 /home/vicuser/data_fetch/scratch_bets.py
0 9,15,21 * * * /usr/bin/python3 /home/vicuser/data_fetch/clean_bets.py
0 8,14,20 * * * /usr/bin/python3 /home/vicuser/data_fetch/fetch_wedstrijden.py
