import runpy

fetch_data = r'DEP-G30\0_run_scripts\Main_Scripts\1_Fetch_Data.py'
clean_data = r'DEP-G30\0_run_scripts\Main_Scripts\2_Clean_Data.py'
control_data = r'DEP-G30\0_run_scripts\Main_Scripts\3_Control_Data.py'


runpy.run_path(fetch_data)
runpy.run_path(clean_data)
runpy.run_path(control_data)
