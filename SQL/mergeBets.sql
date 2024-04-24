use DEP_DWH_G30

DROP TABLE IF EXISTS allbets;

SELECT 
    datum, 
    BetSiteKey, 
    StamnummerThuis, 
    StamnummerUit, 
    OddsThuisWint, 
    OddsUitWint, 
    OddsGelijk,
	OddsBeideTeamsScoren,
	OddsNietBeideTeamsScoren,
	OddsMeerDanXGoals,
	OddsMinderDanXGoals
INTO 
    dbo.allbets
FROM (
    SELECT 
        datum, 
        d.BetSiteKey,
        StamnummerThuis, 
        StamnummerUit, 
        B365ThuisWint AS OddsThuisWint, 
        B365UitWint AS OddsUitWint, 
        B365Gelijkspel AS OddsGelijk,
		0,
		0,
		0,
		0
    FROM 
        dbo.bets_historische_data
	INNER JOIN 
		dbo.DimBetSite d ON d.SiteNaam = 'B365'
    WHERE 
        B365ThuisWint != ''

    UNION ALL

    SELECT 
        datum, 
        d.BetSiteKey,
        StamnummerThuis, 
        StamnummerUit, 
        BWThuisWint AS OddsThuisWint, 
        BWUitWint AS OddsUitWint, 
        BWGelijkspel AS OddsGelijk,
		0,
		0,
		0,
		0
    FROM 
        dbo.bets_historische_data
INNER JOIN 
		dbo.DimBetSite d ON d.SiteNaam = 'BW'
    WHERE 
        BWThuisWint != ''

    UNION ALL

    SELECT 
        datum, 
        d.BetSiteKey,
        StamnummerThuis, 
        StamnummerUit, 
        IWThuisWint AS OddsThuisWint, 
        IWUitWint AS OddsUitWint, 
        IWGelijkspel AS OddsGelijk,
		0,
		0,
		0,
		0
    FROM 
        dbo.bets_historische_data
INNER JOIN 
		dbo.DimBetSite d ON d.SiteNaam = 'IW'
    WHERE 
        IWThuisWint != ''

    UNION ALL

    SELECT 
        datum, 
        d.BetSiteKey,
        StamnummerThuis, 
        StamnummerUit, 
        WHThuisWint AS OddsThuisWint, 
        WHUitWint AS OddsUitWint, 
        WHGelijkspel AS OddsGelijk,
		0,
		0,
		0,
		0
    FROM 
        dbo.bets_historische_data
INNER JOIN 
		dbo.DimBetSite d ON d.SiteNaam = 'WH'
    WHERE 
        WHThuisWint != ''

    UNION ALL

    SELECT 
        datum, 
        d.BetSiteKey, 
        StamnummerThuis, 
        StamnummerUit, 
        VCThuisWint AS OddsThuisWint, 
        VCUitWint AS OddsUitWint, 
        VCGelijkspel AS OddsGelijk,
		0,
		0,
		0,
		0
    FROM 
        dbo.bets_historische_data
INNER JOIN 
		dbo.DimBetSite d ON d.SiteNaam = 'VC'
    WHERE 
        VCThuisWint != ''

	UNION ALL

	SELECT 
		Starttijd,
		d.BetSiteKey, 
		'0',
		'0',
		ThuisPloegWint,
		Gelijk,
		UitPloegWint,
		BeideTeamsScoren,
		NietBeideTeamsScoren,
		OverXGoals,
		OnderXGoals
	FROM 
		dbo.bets
INNER JOIN 
		dbo.DimBetSite d ON d.SiteNaam = 'VC777'
) AS combined_data;
