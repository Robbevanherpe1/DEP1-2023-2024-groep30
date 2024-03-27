USE DEP_DWH_G30;



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

-- Tabel voor Doelpunten
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

-- Tabel voor Wedstrijden
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
    Starttijd DATETIME2, -- Gebruik DATETIME2 voor flexibiliteit
    Thuisploeg NVARCHAR(255),
    Uitploeg NVARCHAR(255),
    Vraag NVARCHAR(255),
    Keuze NVARCHAR(255),
    Kans FLOAT,
    Timestamp DATETIME2 -- Gebruik DATETIME2 voor flexibiliteit
);


-- Importeer CSV-bestanden
-- Importeer Klassement
BULK INSERT dbo.Klassement
FROM 'C:\Users\ayman\OneDrive\Bureaublad\HoGENT\Data Engineer\Python DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\klassement.csv'
WITH
(
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

-- Importeer Doelpunten
BULK INSERT dbo.Doelpunten
FROM 'C:\Users\ayman\OneDrive\Bureaublad\HoGENT\Data Engineer\Python DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\doelpunten.csv'
WITH
(
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

-- Importeer Wedstrijden
BULK INSERT dbo.Wedstrijden
FROM 'C:\Users\ayman\OneDrive\Bureaublad\HoGENT\Data Engineer\Python DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\wedstrijden.csv'
WITH
(
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

-- Importeer Bets
BULK INSERT dbo.Bets
FROM 'C:\Users\ayman\OneDrive\Bureaublad\HoGENT\Data Engineer\Python DEP\DEP1-2023-2024-groep30\transfermarkt\data\correcte_data\bets.csv'
WITH
(
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 1
);








-- Vul DimTeam
DROP SEQUENCE IF EXISTS seq_dt;
CREATE SEQUENCE seq_dt START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimTeam;
GO

INSERT INTO dbo.DimTeam (TeamKey, Stamnummer, ploegnaam)
SELECT
NEXT VALUE FOR seq_dt,
	stamnummer,
	ploeg
FROM (
SELECT DISTINCT
	stamnummer,
	ploeg
FROM dbo.klassement
) AS a;

-- Vul DimDate
DROP SEQUENCE IF EXISTS seq_dd;
CREATE SEQUENCE seq_dd START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimDate;
GO

INSERT INTO dbo.DimDate (
	DateKey,
	Datum,
	DagVanDeMaand,
	DagVanHetJaar,
	WeekVanHetJaar,
	DagVanDeWeek,
	Maand,
	Semester,
	Kwartaal,
	Jaar,
	EngelseDag,
	EngelseMaand,
	DDMMJJJJ
)
SELECT
	NEXT VALUE FOR seq_dd,
	datum,
	DAY(datum),
	DATEPART(DAYOFYEAR, datum),
	DATEPART(WEEK, datum),
	(DATEPART(WEEKDAY, datum) + @@DATEFIRST + 5) % 7, -- maandag = 0 tot zondag = 6
	MONTH(datum),
	CASE WHEN DATEPART(QUARTER, datum) IN (1, 2) THEN 1 ELSE 2 END AS Semester,
	DATEPART(QUARTER, datum),
	YEAR(datum),
	DATENAME(WEEKDAY, datum),
	DATENAME(MONTH, datum),
	RIGHT('0' + CONVERT(VARCHAR, DAY(datum)), 2) + RIGHT('0' + CONVERT(VARCHAR, MONTH(datum)), 2) + CONVERT(VARCHAR, YEAR(datum))
FROM (
    SELECT DISTINCT 
        datum
    FROM dbo.wedstrijden
) AS b;

-- Vul DimKans
DROP SEQUENCE IF EXISTS seq_dk;
CREATE SEQUENCE seq_dk START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimKans;
GO

INSERT INTO dbo.DimKans(KansKey, OddsWaarde)
SELECT 
    NEXT VALUE FOR seq_dk, 
    OddsWaarde
FROM (
    VALUES 
    (1.5), 
    (2.5), 
    (3.5)
) AS c(OddsWaarde);

-- Vul DimTime
DROP SEQUENCE IF EXISTS seq_dt;
CREATE SEQUENCE seq_dt START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimTime;
GO

INSERT INTO dbo.DimTime(TimeKey, Uur, Minuten, VolledigeTijd, AMPMIndicator, UurVanDeDagInMinuten, UurVanDeDagInSeconden)
SELECT 
    NEXT VALUE FOR seq_dt,
    CAST(LEFT(tijdstip, CHARINDEX(':', tijdstip) - 1) AS INT),
    CAST(SUBSTRING(tijdstip, CHARINDEX(':', tijdstip) + 1, 2) AS INT), 
    Tijdstip,
    CASE WHEN CAST(LEFT(tijdstip, CHARINDEX(':', tijdstip) - 1) AS INT) < 12 THEN 'AM' ELSE 'PM' END AS AMPMIndicator,
    (CAST(LEFT(tijdstip, CHARINDEX(':', tijdstip) - 1) AS INT) * 60) + CAST(SUBSTRING(tijdstip, CHARINDEX(':', tijdstip) + 1, 2) AS INT) AS UurVanDeDagInMinuten,
    ((CAST(LEFT(tijdstip, CHARINDEX(':', tijdstip) - 1) AS INT) * 60) + CAST(SUBSTRING(tijdstip, CHARINDEX(':', tijdstip) + 1, 2) AS INT)) * 60 AS UurVanDeDagInSeconden
FROM (
    SELECT DISTINCT 
        tijdstip
    FROM dbo.wedstrijden
) AS d;

-- Vul DimWedstrijd
DROP SEQUENCE IF EXISTS seq_dw;
CREATE SEQUENCE seq_dw START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimWedstrijd;
GO
 
INSERT INTO dbo.DimWedstrijd(WedstrijdKey, MatchID)
SELECT
    NEXT VALUE FOR seq_dw,
    Id
FROM (
    SELECT DISTINCT 
		Id
    FROM dbo.wedstrijden
) AS e;

-- Vul FactWedstrijdScore
DROP SEQUENCE IF EXISTS seq_fw;
CREATE SEQUENCE seq_fw START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.FactWedstrijdScore;
GO

INSERT INTO dbo.FactWedstrijdScore(WedstrijdScoreKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, DateKey, TimeKey, ScoreThuis, 
									ScoreUit, EindscoreThuis, EindscoreUit, ScorendePloegIndicator)
SELECT 
   NEXT VALUE FOR seq_fw,  
   uit.TeamKey,
   thuis.teamkey,
   we.WedstrijdKey,
   da.DateKey,
   t.TimeKey,
   ISNULL(d.StandThuis, 0),
   ISNULL(d.StandUit, 0),
   w.FinaleStandThuisploeg,
   w.FinaleStandUitploeg,
   ISNULL(d.RoepnaamScorendePloeg, 0)
FROM dbo.wedstrijden w
	left join dbo.doelpunten d on d.Id = w.Id
	left join dbo.DimDate da on da.Datum = w.datum
	left join dbo.DimTime t on t.VolledigeTijd = w.Tijdstip
	left join dbo.DimWedstrijd we on we.MatchID = w.id
	left join dbo.DimTeam uit on w.RoepnaamUitploeg = uit.PloegNaam
	left join dbo.DimTeam thuis on w.RoepnaamThuisploeg = thuis.PloegNaam

-- Vul FactKlassement
DROP SEQUENCE IF EXISTS seq_fk;
CREATE SEQUENCE seq_fk START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.FactKlassement;
GO

-- Insert into FactKlassement
INSERT INTO dbo.FactKlassement(KlassementKey, BeginDateKey, EindeDateKey, TeamKey, Stand, AantalGespeeld, AantalGewonnen, AantalGelijk, 
								AantalVerloren, DoelpuntenVoor, DoelpuntenTegen, DoelpuntenSaldo, PuntenVoor2ptn, PuntenTegen2ptn, PuntenVoor3ptn)
SELECT
    NEXT VALUE FOR seq_fk,
	k.Seizoen,
    k.Seizoen + 1,
	t.TeamKey,
	k.Stand,
	k.AantalGespeeld,
	k.AantalGewonnen,
	k.AantalGelijk,
	k.AantalVerloren,
	k.DoelpuntenVoor,
	k.DoelpuntenTegen,
	k.Doelpuntensaldo,
	k.Links_Tweepuntensysteem,
	k.Rechts_Tweepuntensysteem,
	k.Driepuntensysteem
FROM dbo.klassement k
	left join dbo.DimTeam t on t.PloegNaam = k.Ploeg

-- Vul FactWeddenschap
DROP SEQUENCE IF EXISTS seq_fws;
CREATE SEQUENCE seq_fws START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.FactWeddenschap;
GO

INSERT INTO dbo.FactWeddenschapOddsThuisWint, OddsUitWint, OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)

SELECT
    NEXT VALUE FOR seq_fws,
    uit.TeamKey AS TeamKeyUit,
    thuis.TeamKey AS TeamKeyThuis,
    '0' AS WedstrijdKey,
    '0' AS KansKey,
    '0' AS DateKeyScrape,
    '0' AS TimeKeyScrape,
    CONVERT(INT, REPLACE(CONVERT(VARCHAR, b.Starttijd, 112), '-', '')) AS DateKeySpeeldatum,
    REPLACE(CONVERT(VARCHAR, b.Starttijd, 108), ':', '') AS TimeKeySpeeldatum,
    MAX(CASE WHEN b.Vraag = 'Wedstrijduitslag' AND b.Keuze = '1' THEN b.Kans ELSE NULL END) AS OddsThuisWint,
    MAX(CASE WHEN b.Vraag = 'Wedstrijduitslag' AND b.Keuze = '2' THEN b.Kans ELSE NULL END) AS OddsUitWint,
    MAX(CASE WHEN b.Vraag = 'Wedstrijduitslag' AND b.Keuze = 'Gelijkspel' THEN b.Kans ELSE NULL END) AS OddsGelijk,
    MAX(CASE WHEN b.Vraag = 'Beide teams zullen scoren' AND b.Keuze = 'Ja' THEN b.Kans ELSE NULL END) AS OddsBeideTeamsScoren,
    MAX(CASE WHEN b.Vraag = 'Beide teams zullen scoren' AND b.Keuze = 'Nee' THEN b.Kans ELSE NULL END) AS OddsNietBeideTeamsScoren,
    MAX(CASE WHEN b.Vraag = 'Totaal aantal goals' AND b.Keuze LIKE 'Meer dan%' THEN b.Kans ELSE NULL END) AS OddsMeerDanXGoals,
    MAX(CASE WHEN b.Vraag = 'Totaal aantal goals' AND b.Keuze LIKE 'Onder%' THEN b.Kans ELSE NULL END) AS OddsMinderDanXGoals
FROM
    dbo.bets b
    LEFT JOIN dbo.DimTeam uit ON uit.PloegNaam = b.Uitploeg
    LEFT JOIN dbo.DimTeam thuis ON thuis.PloegNaam = b.Thuisploeg
GROUP BY
    uit.TeamKey,
    thuis.TeamKey,
    b.Starttijd