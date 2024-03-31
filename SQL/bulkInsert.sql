USE DEP_DWH_G30;


-- Aanmaken tabellen voor bulk insert
CREATE TABLE dbo.klassement (
    Seizoen INT,
    Speeldag INT,
    Stand INT,
    Stamnummer INT,
    Ploeg NVARCHAR(255),
    AantalGespeeld INT,
    AantalGewonnen INT,
    AantalGelijk INT,
    AantalVerloren INT,
    DoelpuntenVoor INT,
    DoelpuntenTegen INT,
    Doelpuntensaldo INT,
    Links_Tweepuntensysteem INT,
    Rechts_Tweepuntensysteem INT,
    Driepuntensysteem INT
);

CREATE TABLE dbo.doelpunten (
    Seizoen INT,
    Speeldag INT,
    Datum DATE,
    Tijdstip TIME,
    Id INT,
    StamnummerThuisploeg INT,
    RoepnaamThuisploeg NVARCHAR(255),
    StamnummerUitploeg INT,
    RoepnaamUitploeg NVARCHAR(255),
    MinDoelpunt INT,
    TijdstipDoelpunt TIME,
    StamnummerScorendePloeg INT,
    RoepnaamScorendePloeg NVARCHAR(255),
    StandThuis INT,
    StandUit INT
);

CREATE TABLE dbo.wedstrijden (
    Seizoen INT,
    Speeldag INT,
    Datum DATE,
    Tijdstip TIME,
    Id INT,
    StamnummerThuisploeg INT,
    RoepnaamThuisploeg NVARCHAR(255),
    StamnummerUitploeg INT,
    RoepnaamUitploeg NVARCHAR(255),
    FinaleStandThuisploeg INT,
    FinaleStandUitploeg INT
);

CREATE TABLE dbo.bets (
    ID NVARCHAR(255),
    Wedstrijd NVARCHAR(255),
    Starttijd DATETIME2,
    Thuisploeg NVARCHAR(255),
    Uitploeg NVARCHAR(255),
    Vraag NVARCHAR(255),
    Keuze NVARCHAR(255),
    Kans FLOAT,
    Timestamp DATETIME2
);


-- Bulk insert van de CSV-bestanden
BULK INSERT dbo.klassement
FROM 'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\klassement.csv'
WITH
(
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

BULK INSERT dbo.doelpunten
FROM 'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\doelpunten.csv'
WITH
(
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

BULK INSERT dbo.wedstrijden
FROM 'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\wedstrijden.csv'
WITH
(
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

BULK INSERT dbo.bets
FROM 'D:\Hogent\Visual Studio Code\DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\bets.csv'
WITH
(
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);