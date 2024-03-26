USE DEP_DWH_G30;

-- fill DimTeam
drop sequence if exists seq_dt;
create sequence seq_dt start with 1 increment by 1;

delete from dbo.DimTeam;
go
 
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
drop sequence if exists seq_dd;
create sequence seq_dd start with 1 increment by 1;

delete from dbo.DimDate;
go

INSERT INTO dbo.DimDate (
    DateKey, 
    Datum, 
    DagVanDeMaand, 
    dagvanhetjaar,
    WeekVanHetJaar, 
    DagVanDeWeekInMaand, 
    DagVanDeWeekInJaar, 
    Maand, 
    Kwartaal, 
    Jaar, 
    EngelseDag, 
    EngelseMaand, 
    EngelsJaar, 
    DDMMJJJJ
)
SELECT 
    NEXT VALUE FOR seq_dd, 
    datum, 
    DAY(datum),
    DATEPART(dayofyear, datum), 
    DATEPART(week, datum), 
    ROW_NUMBER() OVER (PARTITION BY YEAR(datum), MONTH(datum), DATEPART(week, datum) ORDER BY datum), -- Aantal weken in de maand tot de huidige datum
    (DATEPART(weekday, datum) + @@DATEFIRST + 5) % 7, -- maandag = 0 tot zondag = 6
    MONTH(datum),  
    DATEPART(quarter, datum),  
    YEAR(datum),  
    DATENAME(weekday, datum), 
    DATENAME(month, datum), 
    YEAR(datum), 
    REPLACE(CONVERT(char(10), datum, 104), '.', '') -- Formaat staat nog niet in DDMMYYYY
FROM (
    SELECT DISTINCT 
        datum
    FROM dbo.wedstrijden
) AS b;


-- fill DimKans
drop sequence if exists seq_dk;
create sequence seq_dk start with 1 increment by 1;

delete from dbo.DimKans;
go

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
drop sequence if exists seq_dt;
create sequence seq_dt start with 1 increment by 1;

delete from dbo.DimTime;
go

INSERT INTO dbo.DimTime(TimeKey, Uur, Minuten, VolledigeTijd)
SELECT 
    NEXT VALUE FOR seq_dt, 
    CAST(LEFT(tijdstip, CHARINDEX(':', tijdstip) - 1) AS INT),
    CAST(SUBSTRING(tijdstip, CHARINDEX(':', tijdstip) + 1, 2) AS INT), 
	Tijdstip
FROM (
    SELECT DISTINCT 
        tijdstip
    FROM dbo.wedstrijden
) AS d;


-- fill DimWedstrijd
drop sequence if exists seq_dw;
create sequence seq_dw start with 1 increment by 1;

delete from dbo.DimWedstrijd;
go
 
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
drop sequence if exists seq_fw;
create sequence seq_fw start with 1 increment by 1;

delete from dbo.FactWedstrijdScore;
go

INSERT INTO dbo.FactWedstrijdScore(WedstrijdScoreKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, DateKey, TimeKey, ScoreThuis, ScoreUit, EindscoreThuis, EindscoreUit, ScorendePloegKey)
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

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, KansKey, DateKeyScrape, DateKeyScrape, TimeKeyScrape, DateKeySpeeldatum, TimeKeySpeeldatum,
								OddsThuisWint, OddsUitWint, OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)
SELECT
    NEXT VALUE FOR seq_fws

FROM 