# Import packages
import json
import pandas as pd
import numpy as np
import gspread
import datetime as dt

# Open the JSON file containing the data
with open("df_all_brands_data_cat_all.json", mode="r", encoding='utf-8') as f:
    data = json.load(f)
    data = pd.DataFrame(data)
    f.close()

# Publish the results to a Google sheet
sa = gspread.service_account()
sh = sa.open("lukas_demo_output_ferrari_458")
wks = sh.add_worksheet(title=f"Output_{dt.datetime.now()}", rows=len(data), cols=len(data.columns))
wks.update([data.columns.values.tolist()] + data.values.tolist()) # Publish the results to the G-sheet