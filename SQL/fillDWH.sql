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
    REPLACE(CONVERT(char(10), datum, 104), '.', '') -- Formaat DDMMYYYY
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