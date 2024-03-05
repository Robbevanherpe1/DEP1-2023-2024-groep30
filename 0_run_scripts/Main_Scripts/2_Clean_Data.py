import runpy

clean_goals = r'DEP-G30\transfermarkt\data_cleaning\clean_goals.py'
clean_matches = r'DEP-G30\transfermarkt\data_cleaning\clean_matches.py'
clean_stand = r'DEP-G30\transfermarkt\data_cleaning\clean_stand.py'


runpy.run_path(clean_goals)
runpy.run_path(clean_matches)
runpy.run_path(clean_stand)
