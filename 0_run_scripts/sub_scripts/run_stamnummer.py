import runpy

get_wiki = r'DEP-G30\transfermarkt\data_stamnummer\getWikipediaStamnummer.py'
add_stamnummer = r'DEP-G30\transfermarkt\data_stamnummer\Stamnummer_goals.py'

runpy.run_path(get_wiki)
runpy.run_path(add_stamnummer)