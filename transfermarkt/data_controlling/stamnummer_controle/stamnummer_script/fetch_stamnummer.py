import runpy

get_wiki = r'DEP-G30\transfermarkt\data_controlling\stamnummer_controle\stamnummer_script\getWikipediaStamnummer.py'
add_stamnummer = r'DEP-G30\transfermarkt\data_controlling\stamnummer_controle\stamnummer_script\Stamnummer_goals.py'

runpy.run_path(get_wiki)
runpy.run_path(add_stamnummer)