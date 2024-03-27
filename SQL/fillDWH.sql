USE DEP_DWH_G30;

-- fill DimTeam
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


-- fill DimDate
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


-- fill DimKans
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


-- fill DimTime
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




-- fill DimWedstrijd
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


-- fill FactWedstrijdScore
DROP SEQUENCE IF EXISTS seq_fw;
CREATE SEQUENCE seq_fw START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.FactWedstrijdScore;
GO

INSERT INTO dbo.FactWedstrijdScore(WedstrijdScoreKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, DateKey, TimeKey, ScoreThuis, 
									ScoreUit, EindscoreThuis, EindscoreUit, ScorendePloegKey)
SELECT 
   NEXT VALUE FOR seq_fw,  
   uit.TeamKey,
   thuis.teamkey,
   we.WedstrijdKey,
   da.DateKey,
   t.TimeKey,
   d.StandThuis,
   d.StandUit,
   w.FinaleStandThuisploeg,
   w.FinaleStandUitploeg,
   d.RoepnaamScorendePloeg
FROM dbo.wedstrijden w
	left join dbo.doelpunten d on d.Id = w.Id
	left join dbo.DimDate da on da.Datum = w.datum
	left join dbo.DimTime t on t.VolledigeTijd = w.Tijdstip
	left join dbo.DimWedstrijd we on we.MatchID = w.id
	left join dbo.DimTeam uit on w.RoepnaamUitploeg = uit.PloegNaam
	left join dbo.DimTeam thuis on w.RoepnaamThuisploeg = thuis.PloegNaam


-- fill FactKlassement
drop sequence if exists seq_fk;
create sequence seq_fk start with 1 increment by 1;

delete from dbo.FactKlassement;
go

-- Insert into FactKlassement
INSERT INTO dbo.FactKlassement(KlassementKey, BeginDateKey, EindeDateKey, TeamKey, Stand, AantalGespeeld, AantalGewonnen, AantalGelijk, 
								AantalVerloren, DoelpuntenVoor, DoelpuntenTegen, DoelpuntenSaldo, PuntenVoor2ptn, PuntenTegen2ptn, PuntenVoor3ptn)
SELECT
    NEXT VALUE FOR seq_fk,
	k.Seizoen,
    k.Seizoen,
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


-- fill FactWeddenschap
drop sequence if exists seq_fws;
create sequence seq_fws start with 1 increment by 1;

delete from dbo.FactWeddenschap;
go

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, KansKey, DateKeyScrape, TimeKeyScrape, DateKeySpeeldatum, TimeKeySpeeldatum,
								OddsThuisWint, OddsUitWint, OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)
SELECT
    NEXT VALUE FOR seq_fws,
    uit.TeamKey AS TeamKeyUit,
    thuis.TeamKey AS TeamKeyThuis,
    '0' AS WedstrijdKey, -- Placeholder for actual WedstrijdKey if applicable
    '0' AS KansKey, -- Placeholder for actual KansKey if applicable
    '0' AS DateKeyScrape, -- Placeholder for actual DateKeyScrape if applicable
    '0' AS TimeKeyScrape, -- Placeholder for actual TimeKeyScrape if applicable
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