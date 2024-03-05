import runpy

run_stamnummer = r'DEP-G30\0_run_scripts\sub_scripts\run_stamnummer.py'
control_goal = r'DEP-G30\transfermarkt\data_controlling\control_goals.py'
control_match = r'DEP-G30\transfermarkt\data_controlling\control_matches.py'
control_stand = r'DEP-G30\transfermarkt\data_controlling\control_stand.py'


runpy.run_path(run_stamnummer)
runpy.run_path(control_goal)
runpy.run_path(control_match)
runpy.run_path(control_stand)
