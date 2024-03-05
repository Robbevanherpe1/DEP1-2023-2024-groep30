import runpy

fetch_goal = r'DEP-G30\transfermarkt\data_fetch\fetch_goals.py'
fetch_match = r'DEP-G30\transfermarkt\data_fetch\fetch_matches.py'
fetch_stand = r'DEP-G30\transfermarkt\data_fetch\fetch_stand.py'
fetch_bet = r'DEP-G30\bet777\script\fetch_bets.py'


runpy.run_path(fetch_goal)
runpy.run_path(fetch_match)
runpy.run_path(fetch_stand)
runpy.run_path(fetch_bet)
