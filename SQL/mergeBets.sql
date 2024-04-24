use DEP_DWH_G30

DROP TABLE IF EXISTS allbets;

SELECT 
    datum, 
    betkey, 
    StamnummerThuis, 
    StamnummerUit, 
    OddsThuisWint, 
    OddsUitWint, 
    OddsGelijk
INTO 
    dbo.allbets
FROM (
    SELECT 
        datum, 
        'B365' AS betkey, 
        StamnummerThuis, 
        StamnummerUit, 
        B365ThuisWint AS OddsThuisWint, 
        B365UitWint AS OddsUitWint, 
        B365Gelijkspel AS OddsGelijk
    FROM 
        dbo.bets_historische_data
    WHERE 
        B365ThuisWint != ''

    UNION ALL

    SELECT 
        datum, 
        'BW' AS betkey, 
        StamnummerThuis, 
        StamnummerUit, 
        BWThuisWint AS OddsThuisWint, 
        BWUitWint AS OddsUitWint, 
        BWGelijkspel AS OddsGelijk
    FROM 
        dbo.bets_historische_data
    WHERE 
        BWThuisWint != ''

    UNION ALL

    SELECT 
        datum, 
        'IW' AS betkey, 
        StamnummerThuis, 
        StamnummerUit, 
        IWThuisWint AS OddsThuisWint, 
        IWUitWint AS OddsUitWint, 
        IWGelijkspel AS OddsGelijk
    FROM 
        dbo.bets_historische_data
    WHERE 
        IWThuisWint != ''

    UNION ALL

    SELECT 
        datum, 
        'WH' AS betkey, 
        StamnummerThuis, 
        StamnummerUit, 
        WHThuisWint AS OddsThuisWint, 
        WHUitWint AS OddsUitWint, 
        WHGelijkspel AS OddsGelijk
    FROM 
        dbo.bets_historische_data
    WHERE 
        WHThuisWint != ''

    UNION ALL

    SELECT 
        datum, 
        'VC' AS betkey, 
        StamnummerThuis, 
        StamnummerUit, 
        VCThuisWint AS OddsThuisWint, 
        VCUitWint AS OddsUitWint, 
        VCGelijkspel AS OddsGelijk
    FROM 
        dbo.bets_historische_data
    WHERE 
        VCThuisWint != ''

	UNION ALL

	SELECT 
		Starttijd,
		'BET777',
		'0',
		'0',
		ThuisPloegWint,
		Gelijk,
		UitPloegWint
	FROM 
		dbo.bets
) AS combined_data;
