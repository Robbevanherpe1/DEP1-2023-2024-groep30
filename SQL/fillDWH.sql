USE DEP_DWH_G30;

-- Alle tabellen leeg maken
DELETE FROM dbo.FactKlassement;
DELETE FROM dbo.FactWeddenschap;
DELETE FROM dbo.FactWedstrijdScore;
DELETE FROM dbo.DimTeam;
DELETE FROM dbo.DimKans;
DELETE FROM dbo.DimTime;
DELETE FROM dbo.DimDate;
DELETE FROM dbo.DimWedstrijd;
DELETE FROM dbo.DimBetSite;
GO

-- VUL DIMBETSITE
DROP SEQUENCE IF EXISTS seq_bs;
CREATE SEQUENCE seq_bs START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimBetSite;
GO

INSERT INTO dbo.DimBetSite(BetSiteKey, SiteNaam)
SELECT 
    NEXT VALUE FOR seq_bs, 
    SiteNaam
FROM (
    VALUES 
    ('B365'),
    ('BS'),
    ('BW'),
	('IW'),
	('WH'),
	('VC'),
	('Bet777')
) AS g(SiteNaam);


-- VUL DIMTEAM
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


-- VUL DIMKANS
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


-- VUL DIMWEDSTRIJD
DROP SEQUENCE IF EXISTS seq_dw;
CREATE SEQUENCE seq_dw START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimWedstrijd;
GO
 
INSERT INTO dbo.DimWedstrijd(WedstrijdKey, MatchID, PlayOffsIndicator)
SELECT
    NEXT VALUE FOR seq_dw,
    Id,
	0
FROM (
    SELECT DISTINCT 
		Id
    FROM dbo.wedstrijden
) AS e;

INSERT INTO dbo.DimWedstrijd(WedstrijdKey, MatchID, PlayOffsIndicator)
SELECT
    NEXT VALUE FOR seq_dw,
    tf_match_id,
	1
FROM (
    SELECT DISTINCT 
		tf_match_id
    FROM dbo.wedstrijden_playoffs_i_en_ii
) AS e;


-- VUL DIMDATE
DROP SEQUENCE IF EXISTS seq_dd;
CREATE SEQUENCE seq_dd START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimDate;
GO

;WITH DateRange AS (
    SELECT CAST('1960-09-04' AS DATETIME) AS Datum
    UNION ALL
    SELECT DATEADD(DAY, 1, Datum)
    FROM DateRange
    WHERE Datum < CAST(GETDATE() AS DATE)
),
TheorDates AS (
    SELECT
        Seizoen,
        Speeldag,
        CAST(datum AS DATETIME) AS Datum,
        LEAD(CAST(datum AS DATETIME), 1, GETDATE()) OVER (ORDER BY CAST(datum AS DATETIME)) AS NextDate
    FROM dbo.theoretische_speeldagen
)
INSERT INTO dbo.DimDate (
    DateKey, Datum, Seizoen, Speeldag, DagVanDeMaand, DagVanHetJaar, WeekVanHetJaar,
    DagVanDeWeek, Maand, Semester, Kwartaal, Jaar, EngelseDag, EngelseMaand,
    DDMMJJJJ, IsWeekend, NaamDagVanDeWeek, IsSchrikkeljaar, WeekVanDeMaand, Tijdvak
)
SELECT
    NEXT VALUE FOR seq_dd,
    dr.Datum,
    ISNULL(td.Seizoen, 'Onbekend') AS Seizoen,
    ISNULL(td.Speeldag, 0) AS Speeldag,  -- Standaardwaarde hier ingesteld als 0
    DAY(dr.Datum) AS DagVanDeMaand,
    DATEPART(DAYOFYEAR, dr.Datum) AS DagVanHetJaar,
    DATEPART(WEEK, dr.Datum) AS WeekVanHetJaar,
    (DATEPART(WEEKDAY, dr.Datum) + @@DATEFIRST + 5) % 7 AS DagVanDeWeek,
    MONTH(dr.Datum) AS Maand,
    CASE WHEN DATEPART(QUARTER, dr.Datum) IN (1, 2) THEN 1 ELSE 2 END AS Semester,
    DATEPART(QUARTER, dr.Datum) AS Kwartaal,
    YEAR(dr.Datum) AS Jaar,
    DATENAME(WEEKDAY, dr.Datum) AS EngelseDag,
    DATENAME(MONTH, dr.Datum) AS EngelseMaand,
    REPLACE(CONVERT(VARCHAR, dr.Datum, 104), '.', '') AS DDMMJJJJ,
    CASE WHEN ((DATEPART(WEEKDAY, dr.Datum) + @@DATEFIRST - 1) % 7) IN (0, 6) THEN 1 ELSE 0 END AS IsWeekend,
    CASE DATENAME(WEEKDAY, dr.Datum) 
        WHEN 'Sunday' THEN 'Zondag'
        WHEN 'Monday' THEN 'Maandag'
        WHEN 'Tuesday' THEN 'Dinsdag'
        WHEN 'Wednesday' THEN 'Woensdag'
        WHEN 'Thursday' THEN 'Donderdag'
        WHEN 'Friday' THEN 'Vrijdag'
        WHEN 'Saturday' THEN 'Zaterdag'
    END AS NaamDagVanDeWeek,
    CASE WHEN (YEAR(dr.Datum) % 4 = 0 AND YEAR(dr.Datum) % 100 != 0) OR (YEAR(dr.Datum) % 400 = 0) THEN 1 ELSE 0 END AS IsSchrikkeljaar,
    (DAY(dr.Datum) + DATEPART(WEEKDAY, DATEADD(DAY, 1-DAY(dr.Datum), dr.Datum)) - 2) / 7 + 1 AS WeekVanDeMaand,
    CASE 
        WHEN MONTH(dr.Datum) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(dr.Datum) IN (3, 4, 5) THEN 'Lente' 
        WHEN MONTH(dr.Datum) IN (6, 7, 8) THEN 'Zomer' 
        ELSE 'Herfst'
    END AS Tijdvak
FROM DateRange dr
LEFT JOIN TheorDates td ON dr.Datum >= td.Datum AND dr.Datum < td.NextDate
OPTION (MAXRECURSION 0);
GO


-- VUL DIMTIME
DROP SEQUENCE IF EXISTS seq_dt;
CREATE SEQUENCE seq_dt START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.DimTime;
GO

;WITH TimeCTE AS (
    SELECT 0 AS Hour, 0 AS Minute
    UNION ALL
    SELECT CASE WHEN Minute = 59 THEN Hour + 1 ELSE Hour END,
           CASE WHEN Minute = 59 THEN 0 ELSE Minute + 1 END
    FROM TimeCTE
    WHERE Hour < 24 AND (Hour < 23 OR (Hour = 23 AND Minute < 59))
)
INSERT INTO dbo.DimTime(TimeKey, Uur, Minuten, VolledigeTijd, AMPMIndicator, UurVanDeDagInMinuten, UurVanDeDagInSeconden)
SELECT
    NEXT VALUE FOR seq_dt,
    Hour,
    Minute,
    FORMAT(DATEADD(MINUTE, (Hour * 60) + Minute, 0), 'HH:mm') AS VolledigeTijd,
    CASE WHEN Hour < 12 THEN 'AM' ELSE 'PM' END AS AMPMIndicator,
    (Hour * 60) + Minute AS UurVanDeDagInMinuten,
    ((Hour * 60) + Minute) * 60 AS UurVanDeDagInSeconden
FROM TimeCTE
OPTION (MAXRECURSION 32767);

GO


-- Vul FACTWEDSTRIJDSCORE
DROP SEQUENCE IF EXISTS seq_fw;
CREATE SEQUENCE seq_fw START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.FactWedstrijdScore;
GO

INSERT INTO dbo.FactWedstrijdScore(
    WedstrijdScoreKey, 
    TeamKeyUit, 
    TeamKeyThuis, 
    WedstrijdKey, 
    DateKey, 
    TimeKey, 
    ScoreThuis, 
    ScoreUit, 
    EindscoreThuis, 
    EindscoreUit, 
    ScorendePloegIndicator
)
SELECT 
    NEXT VALUE FOR seq_fw, 
    uit.TeamKey,
    thuis.teamkey,
    we.WedstrijdKey,
    ISNULL(da.DateKey, 0),
    t.TimeKey,
    ISNULL(d.StandThuis, 0),
    ISNULL(d.StandUit, 0),
    w.FinaleStandThuisploeg,
    w.FinaleStandUitploeg,
    ISNULL(d.RoepnaamScorendePloeg, 0)
FROM dbo.wedstrijden w
	LEFT JOIN dbo.doelpunten d ON d.Id = w.Id
	LEFT JOIN dbo.DimDate da ON da.Datum = w.Datum
	LEFT JOIN dbo.DimTime t ON t.VolledigeTijd = w.Tijdstip
	LEFT JOIN dbo.DimWedstrijd we ON we.MatchID = w.id
	LEFT JOIN dbo.DimTeam uit ON w.RoepnaamUitploeg = uit.PloegNaam
	LEFT JOIN dbo.DimTeam thuis ON w.RoepnaamThuisploeg = thuis.PloegNaam;
GO

INSERT INTO dbo.FactWedstrijdScore(
    WedstrijdScoreKey, 
    TeamKeyUit, 
    TeamKeyThuis, 
    WedstrijdKey, 
    DateKey, 
    TimeKey, 
    ScoreThuis, 
    ScoreUit, 
    EindscoreThuis, 
    EindscoreUit, 
    ScorendePloegIndicator
)
SELECT 
    NEXT VALUE FOR seq_fw,  
    uit.TeamKey,
    thuis.TeamKey,
    we.WedstrijdKey,
    ISNULL(da.DateKey, 0),
    t.TimeKey,
    ISNULL(wp.stand_thuis, 0),
    ISNULL(wp.stand_uit, 0),
    wp.stand_thuis,
    wp.stand_uit,
    ISNULL(d.roepnaam_scorende_ploeg, 0)
FROM dbo.wedstrijden_playoffs_i_en_ii wp
	LEFT JOIN dbo.doelpunten_playoffs_i_en_ii d ON d.tf_match_id = wp.tf_match_id
    LEFT JOIN dbo.DimDate da ON da.Datum = wp.datum
    LEFT JOIN dbo.DimTime t ON t.VolledigeTijd = wp.tijdstip
    LEFT JOIN dbo.DimWedstrijd we ON we.MatchID = wp.tf_match_id
    LEFT JOIN dbo.DimTeam uit ON uit.PloegNaam = wp.roepnaam_uit
    LEFT JOIN dbo.DimTeam thuis ON thuis.PloegNaam = wp.roepnaam_thuis;
GO


-- VUL KLASSSEMENT
DROP SEQUENCE IF EXISTS seq_fk;
CREATE SEQUENCE seq_fk START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.FactKlassement;
GO

INSERT INTO dbo.FactKlassement(
    KlassementKey, BeginDateKey, EindeDateKey, TeamKey, Stand, AantalGespeeld, AantalGewonnen, AantalGelijk, 
    AantalVerloren, DoelpuntenVoor, DoelpuntenTegen, DoelpuntenSaldo, PuntenVoor2ptn, PuntenTegen2ptn, PuntenVoor3ptn
)
SELECT 
    NEXT VALUE FOR seq_fk,
    ISNULL(bd.DateKey, 0),
    ISNULL(ed.DateKey, 0),
    ISNULL(t.TeamKey, 0),
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
	LEFT JOIN dbo.DimTeam t ON t.PloegNaam = k.Ploeg
	LEFT JOIN dbo.DimDate bd ON bd.Datum = (SELECT MIN(Datum) FROM DimDate WHERE Speeldag= k.Speeldag AND Seizoen = k.Seizoen)
	LEFT JOIN dbo.DimDate ed ON ed.Datum = (SELECT MAX(Datum) FROM DimDate WHERE Speeldag= k.Speeldag AND Seizoen = k.Seizoen)


-- VUL FACTWEDDENSCHAP
DROP SEQUENCE IF EXISTS seq_fws;
CREATE SEQUENCE seq_fws START WITH 1 INCREMENT BY 1;

DELETE FROM dbo.FactWeddenschap;
GO

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, BetSiteKey, DateKeySpeeldatum, OddsThuisWint, OddsUitWint, 
        OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)

 SELECT 
		NEXT VALUE FOR seq_fws,
		uit.TeamKey,
		thuis.TeamKey,
		ISNULL(w.Id, 0),
		b.BetSiteKey,
		d.DateKey,
		h.B365ThuisWint,
		h.B365UitWint,
		h.B365Gelijkspel,
		0,
		0,
		CAST(ISNULL(NULLIF(h.[MeerDoelpunten>2 5], ''), 0) AS decimal(5,2)),
		CAST(ISNULL(NULLIF(h.[MinderDoelpunten<2 5], ''), 0) AS decimal(5,2))
FROM 
    dbo.bets_historische_data h

	LEFT JOIN dbo.DimBetSite b ON b.SiteNaam = 'B365'
	LEFT JOIN dbo.wedstrijden w ON w.id = 
										(		SELECT Id 
												FROM dbo.wedstrijden w 
												WHERE w.StamnummerThuisploeg = h.StamnummerThuis AND w.StamnummerUitploeg = h.StamnummerUit AND w.Datum = h.Datum
										)
	LEFT JOIN dbo.DimTeam uit ON uit.Stamnummer = h.StamnummerUit
	LEFT JOIN dbo.DimTeam thuis ON thuis.Stamnummer = h.StamnummerThuis
	LEFT JOIN dbo.DimDate d ON d.Datum = h.Datum

	WHERE h.B365ThuisWint != '';
GO

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, BetSiteKey, DateKeySpeeldatum, OddsThuisWint, OddsUitWint, 
        OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)

 SELECT 
		NEXT VALUE FOR seq_fws,
		uit.TeamKey,
		thuis.TeamKey,
		ISNULL(w.Id, 0),
		b.BetSiteKey,
		d.DateKey,
		h.BWThuisWint,
		h.BWUitWint,
		h.BWGelijkspel,
		0,
		0,
		CAST(ISNULL(NULLIF(h.[MeerDoelpunten>2 5], ''), 0) AS decimal(5,2)),
		CAST(ISNULL(NULLIF(h.[MinderDoelpunten<2 5], ''), 0) AS decimal(5,2))
FROM 
    dbo.bets_historische_data h

	LEFT JOIN dbo.DimBetSite b ON b.SiteNaam = 'BW'
	LEFT JOIN dbo.wedstrijden w ON w.id = 
										(		SELECT Id 
												FROM dbo.wedstrijden w 
												WHERE w.StamnummerThuisploeg = h.StamnummerThuis AND w.StamnummerUitploeg = h.StamnummerUit AND w.Datum = h.Datum
										)
	LEFT JOIN dbo.DimTeam uit ON uit.Stamnummer = h.StamnummerUit
	LEFT JOIN dbo.DimTeam thuis ON thuis.Stamnummer = h.StamnummerThuis
	LEFT JOIN dbo.DimDate d ON d.Datum = h.Datum

	WHERE h.BWThuisWint != '';
GO

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, BetSiteKey, DateKeySpeeldatum, OddsThuisWint, OddsUitWint, 
        OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)

 SELECT 
		NEXT VALUE FOR seq_fws,
		uit.TeamKey,
		thuis.TeamKey,
		ISNULL(w.Id, 0),
		b.BetSiteKey,
		d.DateKey,
		h.IWThuisWint,
		h.IWUitWint,
		h.IWGelijkspel,
		0,
		0,
		CAST(ISNULL(NULLIF(h.[MeerDoelpunten>2 5], ''), 0) AS decimal(5,2)),
		CAST(ISNULL(NULLIF(h.[MinderDoelpunten<2 5], ''), 0) AS decimal(5,2))
FROM 
    dbo.bets_historische_data h

	LEFT JOIN dbo.DimBetSite b ON b.SiteNaam = 'IW'
	LEFT JOIN dbo.wedstrijden w ON w.id = 
										(		SELECT Id 
												FROM dbo.wedstrijden w 
												WHERE w.StamnummerThuisploeg = h.StamnummerThuis AND w.StamnummerUitploeg = h.StamnummerUit AND w.Datum = h.Datum
										)
	LEFT JOIN dbo.DimTeam uit ON uit.Stamnummer = h.StamnummerUit
	LEFT JOIN dbo.DimTeam thuis ON thuis.Stamnummer = h.StamnummerThuis
	LEFT JOIN dbo.DimDate d ON d.Datum = h.Datum

	WHERE h.IWThuisWint != '';
GO

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, BetSiteKey, DateKeySpeeldatum, OddsThuisWint, OddsUitWint, 
        OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)

 SELECT 
		NEXT VALUE FOR seq_fws,
		uit.TeamKey,
		thuis.TeamKey,
		ISNULL(w.Id, 0),
		b.BetSiteKey,
		d.DateKey,
		h.VCThuisWint,
		h.VCUitWint,
		h.VCGelijkspel,
		0,
		0,
		CAST(ISNULL(NULLIF(h.[MeerDoelpunten>2 5], ''), 0) AS decimal(5,2)),
		CAST(ISNULL(NULLIF(h.[MinderDoelpunten<2 5], ''), 0) AS decimal(5,2))
FROM 
    dbo.bets_historische_data h

	LEFT JOIN dbo.DimBetSite b ON b.SiteNaam = 'VC'
	LEFT JOIN dbo.wedstrijden w ON w.id = 
										(		SELECT Id 
												FROM dbo.wedstrijden w 
												WHERE w.StamnummerThuisploeg = h.StamnummerThuis AND w.StamnummerUitploeg = h.StamnummerUit AND w.Datum = h.Datum
										)
	LEFT JOIN dbo.DimTeam uit ON uit.Stamnummer = h.StamnummerUit
	LEFT JOIN dbo.DimTeam thuis ON thuis.Stamnummer = h.StamnummerThuis
	LEFT JOIN dbo.DimDate d ON d.Datum = h.Datum

	WHERE h.VCThuisWint != '';
GO

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, BetSiteKey, DateKeySpeeldatum, OddsThuisWint, OddsUitWint, 
        OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)

 SELECT 
		NEXT VALUE FOR seq_fws,
		uit.TeamKey,
		thuis.TeamKey,
		ISNULL(w.Id, 0),
		b.BetSiteKey,
		d.DateKey,
		h.WHThuisWint,
		h.WHUitWint,
		h.WHGelijkspel,
		0,
		0,
		CAST(ISNULL(NULLIF(h.[MeerDoelpunten>2 5], ''), 0) AS decimal(5,2)),
		CAST(ISNULL(NULLIF(h.[MinderDoelpunten<2 5], ''), 0) AS decimal(5,2))
FROM 
    dbo.bets_historische_data h

	LEFT JOIN dbo.DimBetSite b ON b.SiteNaam = 'WH'
	LEFT JOIN dbo.wedstrijden w ON w.id = 
										(		SELECT Id 
												FROM dbo.wedstrijden w 
												WHERE w.StamnummerThuisploeg = h.StamnummerThuis AND w.StamnummerUitploeg = h.StamnummerUit AND w.Datum = h.Datum
										)
	LEFT JOIN dbo.DimTeam uit ON uit.Stamnummer = h.StamnummerUit
	LEFT JOIN dbo.DimTeam thuis ON thuis.Stamnummer = h.StamnummerThuis
	LEFT JOIN dbo.DimDate d ON d.Datum = h.Datum

	WHERE h.WHThuisWint != '';
GO

INSERT INTO dbo.FactWeddenschap(WeddenschapKey, TeamKeyUit, TeamKeyThuis, WedstrijdKey, BetSiteKey, DateKeySpeeldatum, OddsThuisWint, OddsUitWint, 
        OddsGelijk, OddsBeideTeamsScoren, OddsNietBeideTeamsScoren, OddsMeerDanXGoals, OddsMinderDanXGoals)
 SELECT 
    NEXT VALUE FOR seq_fws,
    ISNULL(uit.TeamKey, 999999),
    ISNULL(thuis.TeamKey, 999999),
	0,
	bs.BetSiteKey,
	d.DateKey,
	b.ThuisPloegWint,
	b.UitPloegWint,
	b.Gelijk,
	b.BeideTeamsScoren,
	b.NietBeideTeamsScoren,
	b.OverXGoals,
	b.OnderXGoals

FROM
    dbo.bets b
    JOIN dbo.DimTeam uit ON uit.PloegNaam = b.Uitploeg
	JOIN dbo.DimTeam thuis ON thuis.PloegNaam = b.Thuisploeg
	LEFT JOIN dbo.DimBetSite bs ON bs.SiteNaam = 'Bet777'
    LEFT JOIN dbo.DimDate d ON d.Datum = (
        SELECT Datum
        FROM DimDate 
        WHERE Datum = CONVERT(varchar(50), CONVERT(date, b.starttijd, 126), 23)
    );
