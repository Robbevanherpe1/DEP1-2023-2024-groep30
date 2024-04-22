IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DEP_DWH_G30')
BEGIN
  CREATE DATABASE DEP_DWH_G30;
END;
GO

USE DEP_DWH_G30
GO

DROP TABLE IF EXISTS FactWedstrijdScore, FactWeddenschap, FactKlassement;
DROP TABLE IF EXISTS DimKans, DimTeam, DimTime, DimWedstrijd;
DROP TABLE IF EXISTS DimDate;
DROP TABLE IF EXISTS DimBetSite;
GO

CREATE TABLE DimBetSite (
    BetSiteKey INT PRIMARY KEY,
    SiteNaam NVARCHAR(50) NOT NULL
);
GO

CREATE TABLE DimKans (
    KansKey INT PRIMARY KEY,
    OddsWaarde DECIMAL(5,2) NOT NULL
);
GO

CREATE TABLE DimTeam (
    TeamKey INT PRIMARY KEY,
    Stamnummer INT NOT NULL,
    PloegNaam NVARCHAR(50) NOT NULL
);
GO

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
	Datum DATE NOT NULL,
    Seizoen NVARCHAR(50) NOT NULL,
    Speeldag INT NOT NULL,
    DagVanDeMaand INT NOT NULL,
    DagVanHetJaar INT NOT NULL,
    WeekVanHetJaar INT NOT NULL,
    DagVanDeWeek INT NOT NULL,
    Maand INT NOT NULL,
    Semester INT NOT NULL,
    Kwartaal INT NOT NULL,
    Jaar INT NOT NULL,
    EngelseDag NVARCHAR(50) NOT NULL,
    EngelseMaand NVARCHAR(50) NOT NULL,
    DDMMJJJJ NVARCHAR(50) NOT NULL,
    IsWeekend BIT NOT NULL,
    NaamDagVanDeWeek NVARCHAR(50) NOT NULL,
    IsSchrikkeljaar BIT NOT NULL,
    WeekVanDeMaand INT NOT NULL,
    Tijdvak NVARCHAR(50) NOT NULL
);
GO


CREATE TABLE DimTime (
    TimeKey INT PRIMARY KEY,
    Uur INT NOT NULL,
    Minuten INT NOT NULL,
    VolledigeTijd NVARCHAR(50) NOT NULL,
	AMPMIndicator NVARCHAR(50) NOT NULL,
	UurVanDeDagInMinuten INT NOT NULL,
	UurVanDeDagInSeconden INT NOT NULL,
);
GO

CREATE TABLE DimWedstrijd (
    WedstrijdKey INT PRIMARY KEY,
    MatchID INT NOT NULL,
);
GO

CREATE TABLE FactWedstrijdScore (
    WedstrijdScoreKey INT PRIMARY KEY,
    TeamKeyUit INT NOT NULL,
    TeamKeyThuis INT NOT NULL,
    WedstrijdKey INT NOT NULL,
    DateKey INT NOT NULL,
    TimeKey INT NOT NULL,
    ScoreThuis INT NOT NULL,
    ScoreUit INT NOT NULL,
    EindscoreThuis INT NOT NULL,
    EindscoreUit INT NOT NULL,
    ScorendePloegIndicator NVARCHAR(50) NOT NULL,
    FOREIGN KEY (TeamKeyUit) REFERENCES DimTeam(TeamKey),
    FOREIGN KEY (TeamKeyThuis) REFERENCES DimTeam(TeamKey),
    FOREIGN KEY (WedstrijdKey) REFERENCES DimWedstrijd(WedstrijdKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (TimeKey) REFERENCES DimTime(TimeKey)
);
GO

CREATE TABLE FactWeddenschap (
    WeddenschapKey INT PRIMARY KEY,
    TeamKeyUit INT NOT NULL,
    TeamKeyThuis INT NOT NULL,
    WedstrijdKey INT NOT NULL,
    KansKey INT NOT NULL,
    DateKeyScrape INT NOT NULL,
    TimeKeyScrape INT NOT NULL,
    DateKeySpeeldatum INT NOT NULL,
    TimeKeySpeeldatum INT NOT NULL,
    OddsThuisWint DECIMAL(5,2),
    OddsUitWint DECIMAL(5,2),
    OddsGelijk DECIMAL(5,2),
    OddsBeideTeamsScoren DECIMAL(5,2),
    OddsNietBeideTeamsScoren DECIMAL(5,2),
    OddsMeerDanXGoals DECIMAL(5,2),
    OddsMinderDanXGoals DECIMAL(5,2),
    FOREIGN KEY (TeamKeyUit) REFERENCES DimTeam(TeamKey),
    FOREIGN KEY (TeamKeyThuis) REFERENCES DimTeam(TeamKey),
    FOREIGN KEY (WedstrijdKey) REFERENCES DimWedstrijd(WedstrijdKey),
    FOREIGN KEY (KansKey) REFERENCES DimKans(KansKey),
    FOREIGN KEY (DateKeyScrape) REFERENCES DimDate(DateKey),
    FOREIGN KEY (TimeKeyScrape) REFERENCES DimTime(TimeKey),
    FOREIGN KEY (DateKeySpeeldatum) REFERENCES DimDate(DateKey),
    FOREIGN KEY (TimeKeySpeeldatum) REFERENCES DimTime(TimeKey)
);
GO

CREATE TABLE FactKlassement (
    KlassementKey INT PRIMARY KEY,
    BeginDateKey INT NOT NULL,
    EindeDateKey INT NOT NULL,
    TeamKey INT NOT NULL,
    Stand INT NOT NULL,
    AantalGespeeld INT NOT NULL,
    AantalGewonnen INT NOT NULL,
    AantalGelijk INT NOT NULL,
    AantalVerloren INT NOT NULL,
    DoelpuntenVoor INT NOT NULL,
    DoelpuntenTegen INT NOT NULL,
    DoelpuntenSaldo INT NOT NULL,
    PuntenVoor2ptn INT NOT NULL,
    PuntenTegen2ptn INT NOT NULL,
	PuntenVoor3ptn INT NOT NULL,
    FOREIGN KEY (BeginDateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (EindeDateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (TeamKey) REFERENCES DimTeam(TeamKey)
);
GO
