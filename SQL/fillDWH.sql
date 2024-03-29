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


-- Alle tabellen leeg maken
DELETE FROM dbo.FactKlassement;
DELETE FROM dbo.FactWeddenschap;
DELETE FROM dbo.FactWedstrijdScore;
DELETE FROM dbo.DimTeam;
DELETE FROM dbo.DimKans;
DELETE FROM dbo.DimTime;
DELETE FROM dbo.DimDate;
DELETE FROM dbo.DimWedstrijd;
GO


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

INSERT INTO dbo.DimDate (DateKey, Datum, Seizoen, Speeldag, DagVanDeMaand, DagVanHetJaar, WeekVanHetJaar, DagVanDeWeek, Maand, Semester, Kwartaal, Jaar, 
							EngelseDag, EngelseMaand, DDMMJJJJ, IsWeekend, NaamDagVanDeWeek, IsSchrikkeljaar, WeekVanDeMaand, Tijdvak)
SELECT
    NEXT VALUE FOR seq_dd,
    Datum,
    Seizoen,
    Speeldag,
    DAY(datum) AS DagVanDeMaand,
    DATEPART(DAYOFYEAR, datum) AS DagVanHetJaar,
    DATEPART(WEEK, datum) AS WeekVanHetJaar,
    (DATEPART(WEEKDAY, datum) + @@DATEFIRST + 5) % 7 AS DagVanDeWeek, -- maandag = 0 tot zondag = 6
    MONTH(datum) AS Maand,
    CASE WHEN DATEPART(QUARTER, datum) IN (1, 2) THEN 1 ELSE 2 END AS Semester,
    DATEPART(QUARTER, datum) AS Kwartaal,
    YEAR(datum) AS Jaar,
    DATENAME(WEEKDAY, datum) AS EngelseDag,
    DATENAME(MONTH, datum) AS EngelseMaand,
    RIGHT('0' + CONVERT(VARCHAR, DAY(datum)), 2) + RIGHT('0' + CONVERT(VARCHAR, MONTH(datum)), 2) + CONVERT(VARCHAR, YEAR(datum)) AS DDMMJJJJ,
    CASE WHEN ((DATEPART(WEEKDAY, datum) + @@DATEFIRST - 1) % 7) IN (0, 6) THEN 1 ELSE 0 END AS IsWeekend,
    CASE DATENAME(WEEKDAY, datum) 
        WHEN 'Sunday' THEN 'Zondag'
        WHEN 'Monday' THEN 'Maandag'
        WHEN 'Tuesday' THEN 'Dinsdag'
        WHEN 'Wednesday' THEN 'Woensdag'
        WHEN 'Thursday' THEN 'Donderdag'
        WHEN 'Friday' THEN 'Vrijdag'
        WHEN 'Saturday' THEN 'Zaterdag'
    END AS NaamDagVanDeWeek,
    CASE WHEN (YEAR(datum) % 4 = 0 AND YEAR(datum) % 100 != 0) OR (YEAR(datum) % 400 = 0) THEN 1 ELSE 0 END AS IsSchrikkeljaar,
    (DAY(datum) + DATEPART(WEEKDAY, DATEADD(DAY, 1-DAY(datum), datum)) - 2) / 7 + 1 AS WeekVanDeMaand,
    CASE 
		WHEN MONTH(datum) IN (12, 1, 2) THEN 'Winter'
		WHEN MONTH(datum) IN (3, 4, 5) THEN 'Lente' 
		WHEN MONTH(datum) IN (6, 7, 8) THEN 'Zomer' 
	ELSE 'Herfst' END AS Tijdvak
FROM (
    SELECT DISTINCT 
        Seizoen,
        Speeldag,
        datum
    FROM dbo.wedstrijden
) AS b;
GO



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

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, KansKey, DateKeyScrape, TimeKeyScrape, DateKeySpeeldatum, TimeKeySpeeldatum,
								OddsThuisWint, OddsUitWint, OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)
SELECT
	NEXT VALUE FOR seq_fws,
	ISNULL(uit.TeamKey, 0),
    ISNULL(thuis.TeamKey, 0),
    '0' AS WedstrijdKey,
    2 AS KansKey,
	ISNULL(d.datekey, 0),
	ISNULL(t.timekey, 0),
	ISNULL(d2.datekey, 0),
	ISNULL(t2.timekey, 0),
	(SELECT b.Kans FROM dbo.bets WHERE Vraag = 'Wedstrijduitslag' AND b.Keuze = '1' AND uit.PloegNaam = b.Uitploeg AND thuis.PloegNaam = b.Thuisploeg AND t2.UurVanDeDagInMinuten = DATEDIFF(MINUTE, CAST(CONVERT(DATETIME, LEFT(b.timestamp, 10), 105) AS DATETIME), CONVERT(DATETIME, b.timestamp, 105)) 
	AND FORMAT(CONVERT(datetime, b.timestamp, 105), 'ddMMyy') = d2.DDMMJJJJ) AS OddsThuisWint,
	(SELECT b.Kans FROM dbo.bets WHERE Vraag = 'Wedstrijduitslag' AND b.Keuze = '2' AND uit.PloegNaam = b.Uitploeg AND thuis.PloegNaam = b.Thuisploeg AND t2.UurVanDeDagInMinuten = DATEDIFF(MINUTE, CAST(CONVERT(DATETIME, LEFT(b.timestamp, 10), 105) AS DATETIME), CONVERT(DATETIME, b.timestamp, 105)) 
	AND FORMAT(CONVERT(datetime, b.timestamp, 105), 'ddMMyy') = d2.DDMMJJJJ) AS OddsUitWint,
	(SELECT b.Kans FROM dbo.bets WHERE Vraag = 'Wedstrijduitslag' AND b.Keuze = '2' AND uit.PloegNaam = b.Uitploeg AND thuis.PloegNaam = b.Thuisploeg AND t2.UurVanDeDagInMinuten = DATEDIFF(MINUTE, CAST(CONVERT(DATETIME, LEFT(b.timestamp, 10), 105) AS DATETIME), CONVERT(DATETIME, b.timestamp, 105)) 
	AND FORMAT(CONVERT(datetime, b.timestamp, 105), 'ddMMyy') = d2.DDMMJJJJ) AS OddsGelijk,
	(SELECT b.Kans FROM dbo.bets WHERE Vraag = 'Beide teams zullen scoren' AND b.Keuze = 'Ja' AND uit.PloegNaam = b.Uitploeg AND thuis.PloegNaam = b.Thuisploeg AND t2.UurVanDeDagInMinuten = DATEDIFF(MINUTE, CAST(CONVERT(DATETIME, LEFT(b.timestamp, 10), 105) AS DATETIME), CONVERT(DATETIME, b.timestamp, 105)) 
	AND FORMAT(CONVERT(datetime, b.timestamp, 105), 'ddMMyy') = d2.DDMMJJJJ) AS OddsBeideTeamsScoren,
	(SELECT b.Kans FROM dbo.bets WHERE Vraag = 'Beide teams zullen scoren' AND b.Keuze = 'Nee' AND uit.PloegNaam = b.Uitploeg AND thuis.PloegNaam = b.Thuisploeg AND t2.UurVanDeDagInMinuten = DATEDIFF(MINUTE, CAST(CONVERT(DATETIME, LEFT(b.timestamp, 10), 105) AS DATETIME), CONVERT(DATETIME, b.timestamp, 105)) 
	AND FORMAT(CONVERT(datetime, b.timestamp, 105), 'ddMMyy') = d2.DDMMJJJJ) AS OddsNietBeideTeamsScoren,
	(SELECT b.Kans FROM dbo.bets WHERE Vraag = 'Totaal aantal goals' AND b.Keuze LIKE 'Meer dan%' AND uit.PloegNaam = b.Uitploeg AND thuis.PloegNaam = b.Thuisploeg AND t2.UurVanDeDagInMinuten = DATEDIFF(MINUTE, CAST(CONVERT(DATETIME, LEFT(b.timestamp, 10), 105) AS DATETIME), CONVERT(DATETIME, b.timestamp, 105)) 
	AND FORMAT(CONVERT(datetime, b.timestamp, 105), 'ddMMyy') = d2.DDMMJJJJ) AS OddsMeerDanXGoals,
	(SELECT b.Kans FROM dbo.bets WHERE Vraag = 'Totaal aantal goals' AND  b.Keuze LIKE 'Onder%' AND uit.PloegNaam = b.Uitploeg AND thuis.PloegNaam = b.Thuisploeg AND t2.UurVanDeDagInMinuten = DATEDIFF(MINUTE, CAST(CONVERT(DATETIME, LEFT(b.timestamp, 10), 105) AS DATETIME), CONVERT(DATETIME, b.timestamp, 105)) 
	AND FORMAT(CONVERT(datetime, b.timestamp, 105), 'ddMMyy') = d2.DDMMJJJJ) AS OddsMinderDanXGoals
FROM
    dbo.bets b
    LEFT JOIN dbo.DimTeam uit ON uit.PloegNaam = b.Uitploeg
    LEFT JOIN dbo.DimTeam thuis ON thuis.PloegNaam = b.Thuisploeg
	LEFT JOIN dbo.DimDate d ON RIGHT('0' + CONVERT(VARCHAR, DAY(b.starttijd)), 2) + RIGHT('0' + CONVERT(VARCHAR, MONTH(b.starttijd)), 2) + CONVERT(VARCHAR, YEAR(b.starttijd)) = d.DDMMJJJJ
	LEFT JOIN dbo.DimTime t ON DATEDIFF(MINUTE, CAST(b.starttijd AS DATE), b.starttijd) = t.UurVanDeDagInMinuten
	LEFT JOIN dbo.DimDate d2 ON FORMAT(CONVERT(datetime, b.timestamp, 105), 'ddMMyy') = d2.DDMMJJJJ
	LEFT JOIN dbo.DimTime t2 ON DATEDIFF(MINUTE, CAST(CONVERT(DATETIME, LEFT(b.timestamp, 10), 105) AS DATETIME), CONVERT(DATETIME, b.timestamp, 105)) = t2.UurVanDeDagInMinuten